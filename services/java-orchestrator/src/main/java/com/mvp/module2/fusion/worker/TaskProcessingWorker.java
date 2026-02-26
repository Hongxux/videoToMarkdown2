package com.mvp.module2.fusion.worker;

import com.mvp.module2.fusion.common.UserFacingErrorMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.scheduler.LoadBasedScheduler;
import com.mvp.module2.fusion.service.PersonaAwareReadingService;
import com.mvp.module2.fusion.service.PersonaInsightCardService;
import com.mvp.module2.fusion.service.VideoProcessingOrchestrator;
import com.mvp.module2.fusion.websocket.TaskWebSocketHandler;
import com.mvp.module2.fusion.worker.watchdog.TaskWatchdog;
import com.mvp.module2.fusion.worker.watchdog.TaskWatchdogFactory;
import com.mvp.module2.fusion.worker.watchdog.WatchdogSignalCodec;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class TaskProcessingWorker {

    private static final Logger logger = LoggerFactory.getLogger(TaskProcessingWorker.class);

    @Autowired
    private TaskQueueManager taskQueueManager;

    @Autowired
    private VideoProcessingOrchestrator orchestrator;

    @Autowired
    private TaskWebSocketHandler webSocketHandler;

    @Autowired
    private LoadBasedScheduler loadScheduler;

    @Autowired(required = false)
    private PersonaAwareReadingService personaAwareReadingService;

    @Autowired(required = false)
    private PersonaInsightCardService personaInsightCardService;

    @Autowired
    private TaskWatchdogFactory taskWatchdogFactory;

    @Autowired
    private WatchdogSignalCodec watchdogSignalCodec;

    private ExecutorService workerPool;
    private ScheduledExecutorService watchdogScheduler;
    private volatile boolean running = true;
    private Thread dispatcherThread;

    @PostConstruct
    public void start() {
        workerPool = Executors.newFixedThreadPool(4, runnable -> {
            Thread thread = new Thread(runnable, "TaskWorker-" + System.currentTimeMillis());
            thread.setDaemon(true);
            return thread;
        });

        watchdogScheduler = Executors.newScheduledThreadPool(1, runnable -> {
            Thread thread = new Thread(runnable, "TaskWatchdog");
            thread.setDaemon(true);
            return thread;
        });

        dispatcherThread = new Thread(this::dispatchLoop, "TaskDispatcher");
        dispatcherThread.setDaemon(true);
        dispatcherThread.start();

        logger.info("TaskProcessingWorker started");
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (dispatcherThread != null) {
            dispatcherThread.interrupt();
        }
        if (workerPool != null) {
            workerPool.shutdown();
            try {
                workerPool.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException interruptedError) {
                Thread.currentThread().interrupt();
            }
        }
        if (watchdogScheduler != null) {
            watchdogScheduler.shutdownNow();
        }
        logger.info("TaskProcessingWorker stopped");
    }

    private void dispatchLoop() {
        logger.info("Task dispatcher loop started");
        while (running) {
            try {
                if (loadScheduler.getSystemState() == LoadBasedScheduler.SystemState.OVERLOADED) {
                    logger.warn("System overloaded, pause dispatch for 5s");
                    Thread.sleep(5000);
                    continue;
                }

                TaskEntry task = taskQueueManager.pollNextTask(5, TimeUnit.SECONDS);
                if (task != null) {
                    logger.info("Dispatch task: {} ({})", task.taskId, task.priority);
                    workerPool.submit(() -> processTask(task));
                }
            } catch (InterruptedException interruptedError) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception error) {
                logger.error("Dispatcher error", error);
            }
        }
        logger.info("Task dispatcher loop stopped");
    }

    private void processTask(TaskEntry task) {
        logger.info("Processing task: {}", task.taskId);
        try {
            if (taskQueueManager.isTaskCancelled(task.taskId)) {
                finalizeCancelled(task, "任务已取消");
                return;
            }

            String outputDir = task.outputDir != null ? task.outputDir : "./output/" + task.taskId;
            TaskWatchdog watchdog = taskWatchdogFactory.create(task.taskId);

            orchestrator.setProgressCallback((taskId, progress, message) -> {
                if (!task.taskId.equals(taskId)) {
                    return;
                }
                if (taskQueueManager.isTaskCancelled(taskId)) {
                    throw new CancellationException("task cancelled by user");
                }
                TaskWatchdog.Signal signal = watchdogSignalCodec.parse(message);
                watchdog.recordProgress(progress, message, signal);
                String outwardMessage = watchdogSignalCodec.sanitizeForUser(message, signal);
                taskQueueManager.updateProgress(taskId, progress, outwardMessage);
                webSocketHandler.broadcastTaskUpdate(taskId, "PROCESSING", progress, outwardMessage, null);
            });

            VideoProcessingOrchestrator.ProcessingResult result = runWithWatchdog(task, outputDir, watchdog);

            if (taskQueueManager.isTaskCancelled(task.taskId)) {
                finalizeCancelled(task, "任务已取消");
                return;
            }

            if (!result.success) {
                throw new RuntimeException(
                        firstNonBlank(result.errorMessage, "Pipeline returned unsuccessful result without details")
                );
            }

            taskQueueManager.completeTask(task.taskId, result.markdownPath);
            webSocketHandler.broadcastTaskUpdate(task.taskId, "COMPLETED", 1.0, "处理完成", result.markdownPath);
            triggerPersonaArtifactsAfterCompletion(task, result);
            logger.info("Task completed: {} -> {}", task.taskId, result.markdownPath);
        } catch (CancellationException cancelledError) {
            logger.info("Task cancelled during processing: {}", task.taskId);
            finalizeCancelled(task, "任务已取消");
        } catch (Exception error) {
            logger.error("Task failed: {}", task.taskId, error);
            String rawError = extractThrowableMessage(error);
            String userMessage = UserFacingErrorMapper.toUserMessage(rawError);
            taskQueueManager.failTask(task.taskId, rawError);
            webSocketHandler.broadcastTaskUpdate(task.taskId, "FAILED", task.progress, userMessage, null);
        }
    }

    private void triggerPersonaArtifactsAfterCompletion(
            TaskEntry task,
            VideoProcessingOrchestrator.ProcessingResult result
    ) {
        if (personaAwareReadingService == null || result == null) {
            return;
        }
        String markdownPathText = firstNonBlank(result.markdownPath, "");
        if (markdownPathText.isBlank()) {
            return;
        }
        try {
            Path markdownPath = Paths.get(markdownPathText).toAbsolutePath().normalize();
            if (!Files.isRegularFile(markdownPath)) {
                logger.warn("skip persona post-completion hook: markdown not found, taskId={}, path={}", task.taskId, markdownPath);
                return;
            }
            String markdown = Files.readString(markdownPath, StandardCharsets.UTF_8);
            PersonaAwareReadingService.PersonalizedReadingPayload payload = personaAwareReadingService.loadOrCompute(
                    task.taskId,
                    task.userId,
                    markdownPath,
                    markdown
            );
            if (personaInsightCardService == null || payload == null || payload.nodes == null || payload.nodes.isEmpty()) {
                return;
            }
            personaInsightCardService.generateAsync(task.taskId, task.userId, markdownPath, payload.nodes);
        } catch (Exception ex) {
            logger.warn(
                    "post-completion persona artifact generation failed: taskId={} err={}",
                    task.taskId,
                    ex.getMessage()
            );
        }
    }

    private VideoProcessingOrchestrator.ProcessingResult runWithWatchdog(
            TaskEntry task,
            String outputDir,
            TaskWatchdog watchdog
    ) throws Exception {
        if (!watchdog.enabled()) {
            return orchestrator.processVideo(task.taskId, task.videoUrl, outputDir);
        }
        Thread ownerThread = Thread.currentThread();
        AtomicReference<TaskWatchdog.Decision> decisionRef = new AtomicReference<>(TaskWatchdog.Decision.none());
        ScheduledFuture<?> watcher = watchdogScheduler.scheduleAtFixedRate(() -> {
            if (decisionRef.get().action() != TaskWatchdog.Action.NONE) {
                return;
            }
            TaskWatchdog.Decision decision = watchdog.evaluate(System.currentTimeMillis());
            if (decision.action() == TaskWatchdog.Action.NONE) {
                return;
            }
            if (!decisionRef.compareAndSet(TaskWatchdog.Decision.none(), decision)) {
                return;
            }
            if (decision.action() == TaskWatchdog.Action.RESTART) {
                String message = String.format(
                        "阶段长时间无进展，准备重启子步骤（阶段=%s，重启=%d/%d）",
                        decision.stage(),
                        decision.stageRestartCount(),
                        watchdog.maxRestartPerStage()
                );
                logger.warn("[{}] {}", task.taskId, message);
                webSocketHandler.broadcastTaskUpdate(task.taskId, "PROCESSING", task.progress, message, null);
            } else if (decision.action() == TaskWatchdog.Action.FAIL) {
                logger.error("[{}] {}", task.taskId, decision.reason());
                webSocketHandler.broadcastTaskUpdate(
                        task.taskId,
                        "PROCESSING",
                        task.progress,
                        "任务长时间无进展，准备终止当前任务",
                        null
                );
            }
            ownerThread.interrupt();
        }, watchdog.pollIntervalMs(), watchdog.pollIntervalMs(), TimeUnit.MILLISECONDS);

        try {
            int attempt = 0;
            while (true) {
                attempt += 1;
                clearInterruptFlag();
                watchdog.onAttemptStart(attempt);
                decisionRef.set(TaskWatchdog.Decision.none());
                try {
                    VideoProcessingOrchestrator.ProcessingResult result =
                            orchestrator.processVideo(task.taskId, task.videoUrl, outputDir);
                    TaskWatchdog.Decision decision = decisionRef.getAndSet(TaskWatchdog.Decision.none());
                    if (result != null && result.success) {
                        return result;
                    }
                    if (decision.action() == TaskWatchdog.Action.RESTART) {
                        sleepWithCancelCheck(task.taskId, decision.backoffMs());
                        continue;
                    }
                    if (decision.action() == TaskWatchdog.Action.FAIL) {
                        throw new RuntimeException(decision.reason());
                    }
                    return result;
                } catch (CancellationException cancelledError) {
                    throw cancelledError;
                } catch (Exception error) {
                    TaskWatchdog.Decision decision = decisionRef.getAndSet(TaskWatchdog.Decision.none());
                    if (decision.action() == TaskWatchdog.Action.RESTART) {
                        sleepWithCancelCheck(task.taskId, decision.backoffMs());
                        continue;
                    }
                    if (decision.action() == TaskWatchdog.Action.FAIL) {
                        throw new RuntimeException(decision.reason(), error);
                    }
                    throw error;
                } finally {
                    clearInterruptFlag();
                }
            }
        } finally {
            watcher.cancel(true);
        }
    }

    private void sleepWithCancelCheck(String taskId, long sleepMs) {
        if (sleepMs <= 0) {
            return;
        }
        long deadline = System.currentTimeMillis() + sleepMs;
        while (System.currentTimeMillis() < deadline) {
            if (taskQueueManager.isTaskCancelled(taskId)) {
                throw new CancellationException("task cancelled during watchdog backoff");
            }
            long remaining = deadline - System.currentTimeMillis();
            long chunk = Math.min(remaining, 500L);
            try {
                Thread.sleep(Math.max(1L, chunk));
            } catch (InterruptedException interruptedError) {
                Thread.currentThread().interrupt();
                throw new CancellationException("watchdog backoff interrupted");
            }
        }
    }

    private void clearInterruptFlag() {
        if (Thread.currentThread().isInterrupted()) {
            Thread.interrupted();
        }
    }

    private void finalizeCancelled(TaskEntry task, String message) {
        taskQueueManager.finalizeCancelledTask(task.taskId);
        webSocketHandler.broadcastTaskUpdate(task.taskId, "CANCELLED", task.progress, message, null);
    }

    private String firstNonBlank(String value, String fallback) {
        if (value != null && !value.isBlank()) {
            return value;
        }
        return fallback;
    }

    private String extractThrowableMessage(Throwable throwable) {
        if (throwable == null) {
            return "Task failed with unknown error";
        }
        Throwable cursor = throwable;
        String fallbackType = throwable.getClass().getSimpleName();
        int depth = 0;
        while (cursor != null && depth < 8) {
            String message = cursor.getMessage();
            if (message != null && !message.isBlank()) {
                if (depth == 0) {
                    return message;
                }
                return cursor.getClass().getSimpleName() + ": " + message;
            }
            fallbackType = cursor.getClass().getSimpleName();
            cursor = cursor.getCause();
            depth += 1;
        }
        return fallbackType + " (message unavailable)";
    }
}
