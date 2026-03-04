package com.mvp.module2.fusion.worker;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.service.VideoProcessingOrchestrator;
import com.mvp.module2.fusion.worker.watchdog.TaskWatchdog;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskProcessingWorkerDownloadRetryTest {

    @Test
    void runWithWatchdogShouldRetryWhenDownloadWaitInterrupted() throws Exception {
        TaskProcessingWorker worker = new TaskProcessingWorker();
        VideoProcessingOrchestrator orchestrator = mock(VideoProcessingOrchestrator.class);
        TaskQueueManager queueManager = new TaskQueueManager(1);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        try {
            setField(worker, "orchestrator", orchestrator);
            setField(worker, "taskQueueManager", queueManager);
            setField(worker, "watchdogScheduler", scheduler);
            setField(worker, "downloadInterruptRetryMaxRetries", 2);
            setField(worker, "downloadInterruptRetryBackoffMs", 0L);

            TaskEntry task = new TaskEntry();
            task.taskId = "task-download-retry-success";
            task.videoUrl = "https://example.com/video";
            String outputDir = "./output/task-download-retry-success";
            TaskWatchdog watchdog = buildStableWatchdog(task.taskId);

            RuntimeException interruptedFailure = new RuntimeException(
                    "Download stage interrupted while waiting for Python worker response",
                    new InterruptedException("simulated interrupt")
            );
            VideoProcessingOrchestrator.ProcessingResult success = new VideoProcessingOrchestrator.ProcessingResult();
            success.success = true;

            when(orchestrator.processVideo(eq(task.taskId), eq(task.videoUrl), eq(outputDir), isNull()))
                    .thenThrow(interruptedFailure)
                    .thenReturn(success);

            VideoProcessingOrchestrator.ProcessingResult result =
                    invokeRunWithWatchdog(worker, task, outputDir, watchdog);

            assertTrue(result.success);
            verify(orchestrator, times(2)).processVideo(eq(task.taskId), eq(task.videoUrl), eq(outputDir), isNull());
        } finally {
            scheduler.shutdownNow();
            queueManager.shutdown();
        }
    }

    @Test
    void runWithWatchdogShouldFailAfterDownloadInterruptRetryExhausted() throws Exception {
        TaskProcessingWorker worker = new TaskProcessingWorker();
        VideoProcessingOrchestrator orchestrator = mock(VideoProcessingOrchestrator.class);
        TaskQueueManager queueManager = new TaskQueueManager(1);
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        try {
            setField(worker, "orchestrator", orchestrator);
            setField(worker, "taskQueueManager", queueManager);
            setField(worker, "watchdogScheduler", scheduler);
            setField(worker, "downloadInterruptRetryMaxRetries", 1);
            setField(worker, "downloadInterruptRetryBackoffMs", 0L);

            TaskEntry task = new TaskEntry();
            task.taskId = "task-download-retry-fail";
            task.videoUrl = "https://example.com/video";
            String outputDir = "./output/task-download-retry-fail";
            TaskWatchdog watchdog = buildStableWatchdog(task.taskId);

            RuntimeException interruptedFailure = new RuntimeException(
                    "Download stage interrupted while waiting for Python worker response",
                    new InterruptedException("simulated interrupt")
            );
            when(orchestrator.processVideo(eq(task.taskId), eq(task.videoUrl), eq(outputDir), isNull()))
                    .thenThrow(interruptedFailure);

            RuntimeException thrown = assertThrows(
                    RuntimeException.class,
                    () -> invokeRunWithWatchdogUnchecked(worker, task, outputDir, watchdog)
            );

            assertTrue(thrown.getMessage().contains("Download stage interrupted"));
            verify(orchestrator, times(2)).processVideo(eq(task.taskId), eq(task.videoUrl), eq(outputDir), isNull());
        } finally {
            scheduler.shutdownNow();
            queueManager.shutdown();
        }
    }

    private TaskWatchdog buildStableWatchdog(String taskId) {
        return TaskWatchdog.enabled(
                taskId,
                3600,
                3,
                2,
                1,
                600,
                600,
                2.0d,
                List.of(0L),
                Set.of()
        );
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

    private VideoProcessingOrchestrator.ProcessingResult invokeRunWithWatchdogUnchecked(
            TaskProcessingWorker worker,
            TaskEntry task,
            String outputDir,
            TaskWatchdog watchdog
    ) {
        try {
            return invokeRunWithWatchdog(worker, task, outputDir, watchdog);
        } catch (RuntimeException runtimeError) {
            throw runtimeError;
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
