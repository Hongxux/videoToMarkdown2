package com.mvp.module2.fusion.worker;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.service.VideoProcessingOrchestrator;
import com.mvp.module2.fusion.worker.watchdog.TaskWatchdog;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TaskProcessingWorkerIoConcurrencyTest {

    @Test
    void runWithWatchdogShouldAllowParallelDownloadAndSerializeTranscribe() throws Exception {
        TaskProcessingWorker worker = new TaskProcessingWorker();
        VideoProcessingOrchestrator orchestrator = mock(VideoProcessingOrchestrator.class);
        TaskQueueManager queueManager = new TaskQueueManager(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger downloadInFlight = new AtomicInteger(0);
        AtomicInteger downloadMaxInFlight = new AtomicInteger(0);
        AtomicInteger transcribeInFlight = new AtomicInteger(0);
        AtomicInteger transcribeMaxInFlight = new AtomicInteger(0);
        AtomicInteger transcribeCallCount = new AtomicInteger(0);
        AtomicInteger transcribeStartedWhileStage1InFlight = new AtomicInteger(0);
        AtomicInteger stage1CallCount = new AtomicInteger(0);
        AtomicInteger stage1InFlight = new AtomicInteger(0);
        CountDownLatch firstStage1Entered = new CountDownLatch(1);
        CountDownLatch allowFirstStage1Continue = new CountDownLatch(1);
        AtomicInteger llmInFlight = new AtomicInteger(0);
        AtomicInteger llmMaxInFlight = new AtomicInteger(0);

        try {
            setField(worker, "orchestrator", orchestrator);
            setField(worker, "taskQueueManager", queueManager);
            setField(worker, "downloadSemaphore", new Semaphore(3));
            setField(worker, "transcribeSemaphore", new Semaphore(1));
            setField(worker, "phase2Semaphore", new Semaphore(6));

            when(orchestrator.shouldRunBookPipeline(anyString(), isNull())).thenReturn(false);
            when(orchestrator.processVideoDownloadPhase(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
                int current = downloadInFlight.incrementAndGet();
                downloadMaxInFlight.updateAndGet(previous -> Math.max(previous, current));
                try {
                    Thread.sleep(120L);
                    VideoProcessingOrchestrator.IOPhaseResult ioResult = new VideoProcessingOrchestrator.IOPhaseResult();
                    ioResult.taskId = invocation.getArgument(0, String.class);
                    ioResult.videoPath = "mock.mp4";
                    ioResult.outputDir = invocation.getArgument(2, String.class);
                    ioResult.videoDuration = 60d;
                    return ioResult;
                } finally {
                    downloadInFlight.decrementAndGet();
                }
            });
            when(orchestrator.processVideoTranscribePhase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class)))
                    .thenAnswer(invocation -> {
                        int callIndex = transcribeCallCount.incrementAndGet();
                        if (callIndex == 2) {
                            assertTrue(
                                    firstStage1Entered.await(2, TimeUnit.SECONDS),
                                    "first stage1 should already start before second transcribe finishes"
                            );
                            if (stage1InFlight.get() > 0) {
                                transcribeStartedWhileStage1InFlight.incrementAndGet();
                            }
                            allowFirstStage1Continue.countDown();
                        }
                        int current = transcribeInFlight.incrementAndGet();
                        transcribeMaxInFlight.updateAndGet(previous -> Math.max(previous, current));
                        try {
                            Thread.sleep(80L);
                            VideoProcessingOrchestrator.IOPhaseResult ioResult =
                                    invocation.getArgument(1, VideoProcessingOrchestrator.IOPhaseResult.class);
                            ioResult.subtitlePath = "mock.srt";
                            return ioResult;
                        } finally {
                            transcribeInFlight.decrementAndGet();
                        }
                    });
            when(orchestrator.processVideoStage1Phase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class)))
                    .thenAnswer(invocation -> {
                        int callIndex = stage1CallCount.incrementAndGet();
                        stage1InFlight.incrementAndGet();
                        try {
                            if (callIndex == 1) {
                                firstStage1Entered.countDown();
                                allowFirstStage1Continue.await(2, TimeUnit.SECONDS);
                            }
                            Thread.sleep(300L);
                            return invocation.getArgument(1, VideoProcessingOrchestrator.IOPhaseResult.class);
                        } finally {
                            stage1InFlight.decrementAndGet();
                        }
                    });
            when(orchestrator.processVideoLLMPhase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class)))
                    .thenAnswer(invocation -> {
                        int current = llmInFlight.incrementAndGet();
                        llmMaxInFlight.updateAndGet(previous -> Math.max(previous, current));
                        try {
                            Thread.sleep(300L);
                            VideoProcessingOrchestrator.ProcessingResult success = new VideoProcessingOrchestrator.ProcessingResult();
                            success.success = true;
                            return success;
                        } finally {
                            llmInFlight.decrementAndGet();
                        }
                    });

            TaskEntry taskA = new TaskEntry();
            taskA.taskId = "task-io-llm-A";
            taskA.videoUrl = "https://example.com/video-a";
            TaskEntry taskB = new TaskEntry();
            taskB.taskId = "task-io-llm-B";
            taskB.videoUrl = "https://example.com/video-b";

            Future<VideoProcessingOrchestrator.ProcessingResult> futureA = pool.submit(() -> {
                startLatch.await(3, TimeUnit.SECONDS);
                return invokeRunWithWatchdog(
                        worker,
                        taskA,
                        "./output/task-io-llm-A",
                        TaskWatchdog.disabled(taskA.taskId)
                );
            });
            Future<VideoProcessingOrchestrator.ProcessingResult> futureB = pool.submit(() -> {
                startLatch.await(3, TimeUnit.SECONDS);
                return invokeRunWithWatchdog(
                        worker,
                        taskB,
                        "./output/task-io-llm-B",
                        TaskWatchdog.disabled(taskB.taskId)
                );
            });

            startLatch.countDown();
            VideoProcessingOrchestrator.ProcessingResult resultA = futureA.get(5, TimeUnit.SECONDS);
            VideoProcessingOrchestrator.ProcessingResult resultB = futureB.get(5, TimeUnit.SECONDS);

            assertTrue(resultA.success);
            assertTrue(resultB.success);
            assertTrue(downloadMaxInFlight.get() >= 2, "download phase should allow parallel execution");
            assertEquals(1, transcribeMaxInFlight.get(), "transcribe phase should stay strictly serialized");
            assertTrue(
                    transcribeStartedWhileStage1InFlight.get() >= 1,
                    "second transcribe should be allowed to start before first stage1 finishes"
            );
            assertTrue(llmMaxInFlight.get() >= 2, "LLM phase should be able to run in parallel");
            verify(orchestrator).processVideoDownloadPhase(eq(taskA.taskId), eq(taskA.videoUrl), eq("./output/task-io-llm-A"));
            verify(orchestrator).processVideoDownloadPhase(eq(taskB.taskId), eq(taskB.videoUrl), eq("./output/task-io-llm-B"));
            verify(orchestrator).processVideoTranscribePhase(eq(taskA.taskId), any(VideoProcessingOrchestrator.IOPhaseResult.class));
            verify(orchestrator).processVideoTranscribePhase(eq(taskB.taskId), any(VideoProcessingOrchestrator.IOPhaseResult.class));
            verify(orchestrator).processVideoStage1Phase(eq(taskA.taskId), any(VideoProcessingOrchestrator.IOPhaseResult.class));
            verify(orchestrator).processVideoStage1Phase(eq(taskB.taskId), any(VideoProcessingOrchestrator.IOPhaseResult.class));
            verify(orchestrator).processVideoLLMPhase(eq(taskA.taskId), any(VideoProcessingOrchestrator.IOPhaseResult.class));
            verify(orchestrator).processVideoLLMPhase(eq(taskB.taskId), any(VideoProcessingOrchestrator.IOPhaseResult.class));
        } finally {
            pool.shutdownNow();
            queueManager.shutdown();
        }
    }

    @Test
    void runWithWatchdogShouldCapPhase2ConcurrencyByPermit() throws Exception {
        TaskProcessingWorker worker = new TaskProcessingWorker();
        VideoProcessingOrchestrator orchestrator = mock(VideoProcessingOrchestrator.class);
        TaskQueueManager queueManager = new TaskQueueManager(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicInteger llmInFlight = new AtomicInteger(0);
        AtomicInteger llmMaxInFlight = new AtomicInteger(0);

        try {
            setField(worker, "orchestrator", orchestrator);
            setField(worker, "taskQueueManager", queueManager);
            setField(worker, "downloadSemaphore", new Semaphore(3));
            setField(worker, "transcribeSemaphore", new Semaphore(3));
            setField(worker, "phase2Semaphore", new Semaphore(1));

            when(orchestrator.shouldRunBookPipeline(anyString(), isNull())).thenReturn(false);
            when(orchestrator.processVideoDownloadPhase(anyString(), anyString(), anyString())).thenAnswer(invocation -> {
                Thread.sleep(20L);
                VideoProcessingOrchestrator.IOPhaseResult ioResult = new VideoProcessingOrchestrator.IOPhaseResult();
                ioResult.taskId = invocation.getArgument(0, String.class);
                ioResult.videoPath = "mock.mp4";
                ioResult.outputDir = invocation.getArgument(2, String.class);
                ioResult.videoDuration = 30d;
                return ioResult;
            });
            when(orchestrator.processVideoTranscribePhase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class)))
                    .thenAnswer(invocation -> {
                        Thread.sleep(20L);
                        VideoProcessingOrchestrator.IOPhaseResult ioResult =
                                invocation.getArgument(1, VideoProcessingOrchestrator.IOPhaseResult.class);
                        ioResult.subtitlePath = "mock.srt";
                        return ioResult;
                    });
            when(orchestrator.processVideoStage1Phase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class)))
                    .thenAnswer(invocation -> {
                        Thread.sleep(20L);
                        return invocation.getArgument(1, VideoProcessingOrchestrator.IOPhaseResult.class);
                    });
            when(orchestrator.processVideoLLMPhase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class)))
                    .thenAnswer(invocation -> {
                        int current = llmInFlight.incrementAndGet();
                        llmMaxInFlight.updateAndGet(previous -> Math.max(previous, current));
                        try {
                            Thread.sleep(200L);
                            VideoProcessingOrchestrator.ProcessingResult success = new VideoProcessingOrchestrator.ProcessingResult();
                            success.success = true;
                            return success;
                        } finally {
                            llmInFlight.decrementAndGet();
                        }
                    });

            TaskEntry taskA = new TaskEntry();
            taskA.taskId = "task-phase2-cap-A";
            taskA.videoUrl = "https://example.com/video-a";
            TaskEntry taskB = new TaskEntry();
            taskB.taskId = "task-phase2-cap-B";
            taskB.videoUrl = "https://example.com/video-b";

            Future<VideoProcessingOrchestrator.ProcessingResult> futureA = pool.submit(() -> {
                startLatch.await(3, TimeUnit.SECONDS);
                return invokeRunWithWatchdog(
                        worker,
                        taskA,
                        "./output/task-phase2-cap-A",
                        TaskWatchdog.disabled(taskA.taskId)
                );
            });
            Future<VideoProcessingOrchestrator.ProcessingResult> futureB = pool.submit(() -> {
                startLatch.await(3, TimeUnit.SECONDS);
                return invokeRunWithWatchdog(
                        worker,
                        taskB,
                        "./output/task-phase2-cap-B",
                        TaskWatchdog.disabled(taskB.taskId)
                );
            });

            startLatch.countDown();
            VideoProcessingOrchestrator.ProcessingResult resultA = futureA.get(5, TimeUnit.SECONDS);
            VideoProcessingOrchestrator.ProcessingResult resultB = futureB.get(5, TimeUnit.SECONDS);

            assertTrue(resultA.success);
            assertTrue(resultB.success);
            assertEquals(1, llmMaxInFlight.get(), "phase2 permit should cap llm concurrency");
        } finally {
            pool.shutdownNow();
            queueManager.shutdown();
        }
    }

    @Test
    void runWithWatchdogShouldAbortWhenTaskCancelledWhileWaitingDownloadPermit() throws Exception {
        TaskProcessingWorker worker = new TaskProcessingWorker();
        VideoProcessingOrchestrator orchestrator = mock(VideoProcessingOrchestrator.class);
        TaskQueueManager queueManager = new TaskQueueManager(1);

        try {
            setField(worker, "orchestrator", orchestrator);
            setField(worker, "taskQueueManager", queueManager);
            setField(worker, "downloadSemaphore", new Semaphore(0));
            setField(worker, "transcribeSemaphore", new Semaphore(1));
            setField(worker, "phase2Semaphore", new Semaphore(1));
            when(orchestrator.shouldRunBookPipeline(anyString(), isNull())).thenReturn(false);

            TaskEntry task = queueManager.submitTask(
                    "user-a",
                    "https://example.com/video-cancel",
                    null,
                    TaskQueueManager.Priority.NORMAL
            );
            queueManager.cancelTask(task.taskId);

            CancellationException cancellation = assertThrows(
                    CancellationException.class,
                    () -> invokeRunWithWatchdog(
                            worker,
                            task,
                            "./output/task-io-cancel",
                            TaskWatchdog.disabled(task.taskId)
                    )
            );
            assertTrue(cancellation.getMessage().contains("cancelled"));
            verify(orchestrator, never()).processVideoDownloadPhase(anyString(), anyString(), anyString());
            verify(orchestrator, never()).processVideoTranscribePhase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class));
            verify(orchestrator, never()).processVideoStage1Phase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class));
            verify(orchestrator, never()).processVideoLLMPhase(anyString(), any(VideoProcessingOrchestrator.IOPhaseResult.class));
        } finally {
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
