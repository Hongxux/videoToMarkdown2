package com.mvp.module2.fusion.worker;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.service.VideoProcessingOrchestrator;
import com.mvp.module2.fusion.websocket.TaskWebSocketHandler;
import com.mvp.module2.fusion.worker.watchdog.TaskWatchdog;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskProcessingWorkerWatchdogDeferredRestartTest {

    @Test
    void runWithWatchdogShouldNotBroadcastRestartMessageWhenHeartbeatStrongStageRestartIsDeferred() throws Exception {
        TaskProcessingWorker worker = new TaskProcessingWorker();
        VideoProcessingOrchestrator orchestrator = mock(VideoProcessingOrchestrator.class);
        TaskQueueManager queueManager = new TaskQueueManager(1);
        TaskWebSocketHandler webSocketHandler = mock(TaskWebSocketHandler.class);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        try {
            setField(worker, "orchestrator", orchestrator);
            setField(worker, "taskQueueManager", queueManager);
            setField(worker, "watchdogScheduler", scheduler);
            setField(worker, "webSocketHandler", webSocketHandler);
            setField(worker, "downloadInterruptRetryMaxRetries", 0);
            setField(worker, "downloadInterruptRetryBackoffMs", 0L);

            TaskEntry task = new TaskEntry();
            task.taskId = "task-stage1-deferred-restart";
            task.videoUrl = "https://example.com/video";
            String outputDir = "./output/task-stage1-deferred-restart";
            TaskWatchdog watchdog = TaskWatchdog.enabled(
                    task.taskId,
                    60,
                    2,
                    1,
                    1,
                    1,
                    1,
                    1.0d,
                    List.of(0L),
                    Set.of("stage1")
            );

            VideoProcessingOrchestrator.IOPhaseResult ioPhaseResult = new VideoProcessingOrchestrator.IOPhaseResult();
            VideoProcessingOrchestrator.ProcessingResult success = new VideoProcessingOrchestrator.ProcessingResult();
            success.success = true;

            when(orchestrator.shouldRunBookPipeline(eq(task.videoUrl), isNull())).thenReturn(false);
            when(orchestrator.processVideoDownloadPhase(eq(task.taskId), eq(task.videoUrl), eq(outputDir)))
                    .thenReturn(ioPhaseResult);
            when(orchestrator.processVideoTranscribePhase(eq(task.taskId), eq(ioPhaseResult)))
                    .thenReturn(ioPhaseResult);
            when(orchestrator.processVideoStage1Phase(eq(task.taskId), eq(ioPhaseResult)))
                    .thenAnswer(invocation -> {
                        watchdog.recordProgress(
                                0.25d,
                                "WATCHDOG_SIGNAL|{\"stage\":\"stage1\",\"status\":\"running\",\"checkpoint\":\"pipeline_pending\",\"completed\":0,\"pending\":6,\"seq\":1,\"signal_type\":\"soft\"}",
                                new TaskWatchdog.Signal("stage1", "running", "pipeline_pending", 0, 6, 1L, "soft")
                        );
                        Thread.sleep(1500L);
                        return ioPhaseResult;
                    });
            when(orchestrator.processVideoLLMPhase(eq(task.taskId), eq(ioPhaseResult))).thenReturn(success);

            VideoProcessingOrchestrator.ProcessingResult result =
                    invokeRunWithWatchdog(worker, task, outputDir, watchdog);

            assertTrue(result.success);
            verify(webSocketHandler, never()).broadcastTaskUpdate(
                    eq(task.taskId),
                    eq("PROCESSING"),
                    anyDouble(),
                    contains("准备重启子步骤"),
                    isNull()
            );
        } finally {
            scheduler.shutdownNow();
            queueManager.shutdown();
        }
    }

    private VideoProcessingOrchestrator.ProcessingResult invokeRunWithWatchdog(
            TaskProcessingWorker worker,
            TaskEntry task,
            String outputDir,
            TaskWatchdog watchdog
    ) throws Exception {
        Method method = TaskProcessingWorker.class.getDeclaredMethod(
                "runWithWatchdog",
                TaskEntry.class,
                String.class,
                TaskWatchdog.class
        );
        method.setAccessible(true);
        try {
            return (VideoProcessingOrchestrator.ProcessingResult) method.invoke(worker, task, outputDir, watchdog);
        } catch (InvocationTargetException invocationError) {
            Throwable cause = invocationError.getCause();
            if (cause instanceof RuntimeException runtimeError) {
                throw runtimeError;
            }
            if (cause instanceof Exception checkedError) {
                throw checkedError;
            }
            throw new RuntimeException(cause);
        }
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}