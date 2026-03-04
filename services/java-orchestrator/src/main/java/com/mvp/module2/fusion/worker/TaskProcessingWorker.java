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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
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
    private static final String DOWNLOAD_INTERRUPTED_WAIT_MESSAGE =
            "Download stage interrupted while waiting for Python worker response";

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

    @Value("${task.queue.max-concurrent:1}")
    private int configuredWorkerConcurrency;

    @Value("${task.upload.dir:var/uploads}")
    private String uploadDir;

    @Value("${task.storage.root:}")
    private String configuredStorageRoot;

    @Value("${video.download.interrupt-retry-max-retries:2}")
    private int downloadInterruptRetryMaxRetries;

    @Value("${video.download.interrupt-retry-backoff-ms:1200}")
    private long downloadInterruptRetryBackoffMs;

    private ExecutorService workerPool;
    private ScheduledExecutorService watchdogScheduler;
    private volatile boolean running = true;
    private Thread dispatcherThread;

    @PostConstruct
    public void start() {
        int workerConcurrency = Math.max(1, configuredWorkerConcurrency);
        workerPool = Executors.newFixedThreadPool(workerConcurrency, runnable -> {
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

        logger.info(
                "TaskProcessingWorker started with concurrency {} (configured={})",
                workerConcurrency,
                configuredWorkerConcurrency
        );
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

            orchestrator.setProgressCallback(task.taskId, (taskId, progress, message) -> {
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

            taskQueueManager.updateCleanupSourcePath(task.taskId, result.cleanupSourcePath);
            taskQueueManager.completeTask(task.taskId, result.markdownPath);
            webSocketHandler.broadcastTaskUpdate(task.taskId, "COMPLETED", 1.0, "处理完成", result.markdownPath);
            triggerPersonaArtifactsAfterCompletion(task, result);
            cleanupUploadedSourceAfterCompletion(task);
            cleanupDownloadedSourceAfterCompletion(task, result);
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
        } finally {
            orchestrator.clearProgressCallback(task.taskId);
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
            return orchestrator.processVideo(
                    task.taskId,
                    task.videoUrl,
                    outputDir,
                    buildBookProcessingOptions(task)
            );
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
            if (decision.action() == TaskWatchdog.Action.RESTART
                    && !watchdog.shouldInterruptOnRestart(decision.stage())) {
                logger.info(
                        "[{}] Watchdog restart deferred for heartbeat-strong stage: stage={}",
                        task.taskId,
                        decision.stage()
                );
                return;
            }
            ownerThread.interrupt();
        }, watchdog.pollIntervalMs(), watchdog.pollIntervalMs(), TimeUnit.MILLISECONDS);

        try {
            int attempt = 0;
            int downloadInterruptRetryCount = 0;
            while (true) {
                attempt += 1;
                clearInterruptFlag();
                watchdog.onAttemptStart(attempt);
                decisionRef.set(TaskWatchdog.Decision.none());
                try {
                    VideoProcessingOrchestrator.ProcessingResult result =
                            orchestrator.processVideo(
                                    task.taskId,
                                    task.videoUrl,
                                    outputDir,
                                    buildBookProcessingOptions(task)
                            );
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
                    if (shouldRetryInterruptedDownload(error, downloadInterruptRetryCount + 1)) {
                        downloadInterruptRetryCount += 1;
                        long backoffMs = resolveDownloadInterruptRetryBackoffMs(downloadInterruptRetryCount);
                        logger.warn(
                                "[{}] Download wait interrupted unexpectedly, trigger idempotent retry {}/{} (attempt={}, backoff={}ms): {}",
                                task.taskId,
                                downloadInterruptRetryCount,
                                Math.max(0, downloadInterruptRetryMaxRetries),
                                attempt,
                                backoffMs,
                                extractThrowableMessage(error)
                        );
                        sleepWithCancelCheck(task.taskId, backoffMs);
                        continue;
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

    private boolean shouldRetryInterruptedDownload(Throwable error, int nextRetryAttempt) {
        if (nextRetryAttempt <= 0) {
            return false;
        }
        if (nextRetryAttempt > Math.max(0, downloadInterruptRetryMaxRetries)) {
            return false;
        }
        return containsDownloadInterruptedWaitMarker(error);
    }

    private long resolveDownloadInterruptRetryBackoffMs(int retryAttempt) {
        long baseMs = Math.max(0L, downloadInterruptRetryBackoffMs);
        if (baseMs <= 0L) {
            return 0L;
        }
        int safeAttempt = Math.max(1, retryAttempt);
        long multiplier = 1L << Math.min(6, safeAttempt - 1);
        long candidate = baseMs * multiplier;
        if (candidate < 0L) {
            return baseMs;
        }
        return Math.min(candidate, 30_000L);
    }

    private boolean containsDownloadInterruptedWaitMarker(Throwable error) {
        Throwable cursor = error;
        int depth = 0;
        while (cursor != null && depth < 10) {
            String message = cursor.getMessage();
            if (message != null && message.contains(DOWNLOAD_INTERRUPTED_WAIT_MESSAGE)) {
                return true;
            }
            cursor = cursor.getCause();
            depth += 1;
        }
        return false;
    }

    private VideoProcessingOrchestrator.BookProcessingOptions buildBookProcessingOptions(TaskEntry task) {
        if (task == null || task.bookOptions == null) {
            return null;
        }
        VideoProcessingOrchestrator.BookProcessingOptions options = new VideoProcessingOrchestrator.BookProcessingOptions();
        options.chapterSelector = task.bookOptions.chapterSelector;
        options.sectionSelector = task.bookOptions.sectionSelector;
        options.splitByChapter = task.bookOptions.splitByChapter;
        options.splitBySection = task.bookOptions.splitBySection;
        options.pageOffset = task.bookOptions.pageOffset;
        if ((options.chapterSelector == null || options.chapterSelector.isBlank())
                && (options.sectionSelector == null || options.sectionSelector.isBlank())
                && options.splitByChapter == null
                && options.splitBySection == null
                && options.pageOffset == null) {
            return null;
        }
        return options;
    }

    private void finalizeCancelled(TaskEntry task, String message) {
        taskQueueManager.finalizeCancelledTask(task.taskId);
        webSocketHandler.broadcastTaskUpdate(task.taskId, "CANCELLED", task.progress, message, null);
    }

    private void cleanupUploadedSourceAfterCompletion(TaskEntry task) {
        if (task == null || task.videoUrl == null || task.videoUrl.isBlank()) {
            return;
        }
        Path uploadRootPath = resolveUploadRootPath();
        if (uploadRootPath == null) {
            return;
        }
        Path sourcePath = resolveLocalSourcePath(task.videoUrl);
        if (sourcePath == null) {
            return;
        }
        if (!isUnderPath(sourcePath, uploadRootPath)) {
            return;
        }
        if (hasOtherActiveTaskUsingSameSource(task.taskId, sourcePath)) {
            logger.info(
                    "Skip uploaded source cleanup because another active task still references it: taskId={} path={}",
                    task.taskId,
                    sourcePath
            );
            return;
        }
        try {
            if (!Files.isRegularFile(sourcePath)) {
                return;
            }
            boolean deleted = Files.deleteIfExists(sourcePath);
            if (deleted) {
                logger.info("Uploaded source cleaned after completion: taskId={} path={}", task.taskId, sourcePath);
            }
        } catch (Exception ex) {
            logger.warn(
                    "Uploaded source cleanup failed: taskId={} path={} err={}",
                    task.taskId,
                    sourcePath,
                    ex.getMessage()
            );
        }
    }

    private void cleanupDownloadedSourceAfterCompletion(
            TaskEntry task,
            VideoProcessingOrchestrator.ProcessingResult result
    ) {
        if (task == null || result == null || result.cleanupSourcePath == null || result.cleanupSourcePath.isBlank()) {
            return;
        }
        Path storageRootPath = resolveStorageRootPath();
        if (storageRootPath == null) {
            return;
        }
        Path sourcePath = resolveLocalSourcePath(result.cleanupSourcePath);
        if (sourcePath == null) {
            return;
        }
        if (!isUnderPath(sourcePath, storageRootPath)) {
            logger.warn(
                    "Skip downloaded source cleanup because path is outside storage root: taskId={} path={} storageRoot={}",
                    task.taskId,
                    sourcePath,
                    storageRootPath
            );
            return;
        }
        if (hasOtherActiveTaskUsingSameSource(task.taskId, sourcePath)) {
            logger.info(
                    "Skip downloaded source cleanup because another active task still references it: taskId={} path={}",
                    task.taskId,
                    sourcePath
            );
            return;
        }
        try {
            if (!Files.isRegularFile(sourcePath)) {
                return;
            }
            boolean deleted = Files.deleteIfExists(sourcePath);
            if (deleted) {
                logger.info("Downloaded source cleaned after completion: taskId={} path={}", task.taskId, sourcePath);
            }
        } catch (Exception ex) {
            logger.warn(
                    "Downloaded source cleanup failed: taskId={} path={} err={}",
                    task.taskId,
                    sourcePath,
                    ex.getMessage()
            );
        }
    }

    private Path resolveUploadRootPath() {
        String configuredUploadDir = uploadDir != null ? uploadDir.trim() : "";
        if (configuredUploadDir.isBlank()) {
            return null;
        }
        try {
            return Paths.get(configuredUploadDir).toAbsolutePath().normalize();
        } catch (InvalidPathException ex) {
            logger.warn(
                    "Skip uploaded source cleanup because upload dir is invalid: dir={} err={}",
                    configuredUploadDir,
                    ex.getMessage()
            );
            return null;
        }
    }

    private Path resolveStorageRootPath() {
        String rawConfiguredStorageRoot = configuredStorageRoot != null ? configuredStorageRoot.trim() : "";
        if (!rawConfiguredStorageRoot.isBlank()) {
            try {
                return Paths.get(rawConfiguredStorageRoot).toAbsolutePath().normalize();
            } catch (InvalidPathException ex) {
                logger.warn(
                        "Skip downloaded source cleanup because storage root is invalid: dir={} err={}",
                        rawConfiguredStorageRoot,
                        ex.getMessage()
                );
                return null;
            }
        }
        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        for (int i = 0; i < 8; i++) {
            Path candidate = current.resolve("var/storage/storage").toAbsolutePath().normalize();
            if (Files.isDirectory(candidate)) {
                return candidate;
            }
            Path parent = current.getParent();
            if (parent == null) {
                break;
            }
            current = parent;
        }
        return Paths.get("var/storage/storage").toAbsolutePath().normalize();
    }

    private Path resolveLocalSourcePath(String rawInput) {
        if (rawInput == null || rawInput.isBlank()) {
            return null;
        }
        String trimmed = rawInput.trim();
        if (isHttpUrl(trimmed)) {
            return null;
        }
        String lower = trimmed.toLowerCase();
        try {
            if (lower.startsWith("file://")) {
                return Paths.get(URI.create(trimmed)).toAbsolutePath().normalize();
            }
            return Paths.get(trimmed).toAbsolutePath().normalize();
        } catch (Exception ex) {
            logger.debug(
                    "Skip source cleanup for unparsable local input: input={} err={}",
                    rawInput,
                    ex.getMessage()
            );
            return null;
        }
    }

    private boolean hasOtherActiveTaskUsingSameSource(String currentTaskId, Path sourcePath) {
        if (taskQueueManager == null || sourcePath == null) {
            return false;
        }
        List<TaskEntry> allTasks = taskQueueManager.getAllTasks();
        for (TaskEntry oneTask : allTasks) {
            if (oneTask == null || oneTask.taskId == null || oneTask.taskId.equals(currentTaskId)) {
                continue;
            }
            if (oneTask.status != TaskQueueManager.TaskStatus.QUEUED
                    && oneTask.status != TaskQueueManager.TaskStatus.PROCESSING) {
                continue;
            }
            Path videoUrlPath = resolveLocalSourcePath(oneTask.videoUrl);
            Path cleanupPath = resolveLocalSourcePath(oneTask.cleanupSourcePath);
            if (sourcePath.equals(videoUrlPath) || sourcePath.equals(cleanupPath)) {
                return true;
            }
        }
        return false;
    }

    private boolean isUnderPath(Path targetPath, Path parentPath) {
        if (targetPath == null || parentPath == null) {
            return false;
        }
        try {
            return targetPath.startsWith(parentPath);
        } catch (Exception ex) {
            return false;
        }
    }

    private boolean isHttpUrl(String value) {
        if (value == null) {
            return false;
        }
        String lower = value.toLowerCase();
        return lower.startsWith("http://") || lower.startsWith("https://");
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
