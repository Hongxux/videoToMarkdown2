package com.mvp.module2.fusion.worker;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.service.TaskDeduplicationService;
import com.mvp.module2.fusion.service.TaskProbeService;
import com.mvp.module2.fusion.service.VideoProcessingOrchestrator;
import com.mvp.module2.fusion.websocket.TaskWebSocketHandler;
import com.mvp.module2.fusion.worker.watchdog.TaskWatchdog;
import com.mvp.module2.fusion.worker.watchdog.TaskWatchdogFactory;
import com.mvp.module2.fusion.worker.watchdog.WatchdogSignalCodec;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskProcessingWorkerProbeConcurrencyTest {

    @Test
    void processTaskShouldStartDownloadWithoutWaitingForRemoteProbe() throws Exception {
        TaskProcessingWorker worker = new TaskProcessingWorker();
        TaskQueueManager queueManager = new TaskQueueManager(1);
        VideoProcessingOrchestrator orchestrator = mock(VideoProcessingOrchestrator.class);
        TaskProbeService taskProbeService = mock(TaskProbeService.class);
        TaskWebSocketHandler webSocketHandler = mock(TaskWebSocketHandler.class);
        TaskWatchdogFactory taskWatchdogFactory = mock(TaskWatchdogFactory.class);
        WatchdogSignalCodec watchdogSignalCodec = mock(WatchdogSignalCodec.class);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        CountDownLatch downloadStarted = new CountDownLatch(1);
        CountDownLatch allowProbeComplete = new CountDownLatch(1);
        CountDownLatch probeFinished = new CountDownLatch(1);
        String asyncTitle = "async probe title";
        Map<String, Object> probePayload = Map.of("resolvedUrl", "https://example.com/video-probe-bg", "title", asyncTitle);

        try {
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
                    "user-probe-bg",
                    "https://example.com/video-probe-bg",
                    "./output/task-probe-bg",
                    TaskQueueManager.Priority.NORMAL
            );
            task.status = TaskQueueManager.TaskStatus.PROBING;
            task.processingSlotAcquired = true;
            task.startedAt = Instant.now();

            when(taskWatchdogFactory.create(task.taskId)).thenReturn(TaskWatchdog.disabled(task.taskId));
            when(orchestrator.shouldRunBookPipeline(anyString(), isNull())).thenReturn(false);
            when(orchestrator.processVideoDownloadPhase(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
                downloadStarted.countDown();
                VideoProcessingOrchestrator.IOPhaseResult ioResult = new VideoProcessingOrchestrator.IOPhaseResult();
                ioResult.taskId = invocation.getArgument(0, String.class);
                ioResult.videoPath = "mock.mp4";
                ioResult.outputDir = invocation.getArgument(2, String.class);
                ioResult.videoDuration = 42d;
                return ioResult;
            });
            when(orchestrator.processVideoTranscribePhase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class)))
                    .thenAnswer(invocation -> invocation.getArgument(1, VideoProcessingOrchestrator.IOPhaseResult.class));
            when(orchestrator.processVideoStage1Phase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class)))
                    .thenAnswer(invocation -> invocation.getArgument(1, VideoProcessingOrchestrator.IOPhaseResult.class));
            when(orchestrator.processVideoLLMPhase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class)))
                    .thenAnswer(invocation -> {
                        VideoProcessingOrchestrator.ProcessingResult success = new VideoProcessingOrchestrator.ProcessingResult();
                        success.success = true;
                        success.markdownPath = "./output/task-probe-bg/result.md";
                        return success;
                    });
            when(taskProbeService.probeTask(task)).thenAnswer(invocation -> {
                try {
                    assertTrue(allowProbeComplete.await(2, TimeUnit.SECONDS), "probe should stay blocked until download already starts");
                    return TaskProbeService.ProbeOutcome.success(
                            asyncTitle,
                            "probe completed",
                            probePayload
                    );
                } finally {
                    probeFinished.countDown();
                }
            });

            Future<?> processFuture = pool.submit(() -> invokeProcessTask(worker, task));

            assertTrue(downloadStarted.await(2, TimeUnit.SECONDS), "download should start without waiting for probe");
            allowProbeComplete.countDown();
            processFuture.get(3, TimeUnit.SECONDS);
            assertTrue(probeFinished.await(2, TimeUnit.SECONDS), "background probe should eventually finish");

            TaskEntry finishedTask = queueManager.getTask(task.taskId);
            assertNotNull(finishedTask);
            assertEquals(TaskQueueManager.TaskStatus.COMPLETED, finishedTask.status);
            assertEquals(asyncTitle, finishedTask.title);
            assertNotNull(finishedTask.probePayload);
            assertEquals(task.videoUrl, String.valueOf(finishedTask.probePayload.get("resolvedUrl")));
            verify(webSocketHandler).broadcastTaskProbeResult(task.taskId, task.userId, probePayload);
        } finally {
            allowProbeComplete.countDown();
            pool.shutdownNow();
            queueManager.shutdown();
        }
    }

    private void invokeProcessTask(TaskProcessingWorker worker, TaskEntry task) {
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

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}