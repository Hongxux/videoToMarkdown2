package com.mvp.module2.fusion.worker;

import com.mvp.module2.fusion.common.UserFacingErrorMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskTransitionResult;
import com.mvp.module2.fusion.scheduler.LoadBasedScheduler;
import com.mvp.module2.fusion.service.PersonaAwareReadingService;
import com.mvp.module2.fusion.service.PersonaInsightCardService;
import com.mvp.module2.fusion.service.TaskDeduplicationService;
import com.mvp.module2.fusion.service.TaskProbeService;
import com.mvp.module2.fusion.service.TaskRuntimeRecoveryService;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
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
    private TaskDeduplicationService taskDeduplicationService;

    @Autowired
    private TaskProbeService taskProbeService;

    @Autowired(required = false)
    private TaskRuntimeRecoveryService taskRuntimeRecoveryService;

    @Autowired
    private TaskWatchdogFactory taskWatchdogFactory;

    @Autowired
    private WatchdogSignalCodec watchdogSignalCodec;

    @Value("${task.queue.max-concurrent:1}")
    private int configuredWorkerConcurrency;

    @Value("${task.pipeline.download-concurrency:3}")
    private int configuredDownloadConcurrency;

    @Value("${task.pipeline.transcribe-concurrency:${task.pipeline.io-concurrency:1}}")
    private int configuredTranscribeConcurrency;

    @Value("${task.pipeline.phase2-concurrency:${task.queue.max-concurrent:6}}")
    private int configuredPhase2Concurrency;

    @Value("${task.pipeline.io-concurrency:1}")
    private int configuredIoConcurrency;

    @Value("${task.upload.dir:var/uploads}")
    private String uploadDir;

    @Value("${task.storage.root:}")
    private String configuredStorageRoot;

    @Value("${video.download.interrupt-retry-max-retries:2}")
    private int downloadInterruptRetryMaxRetries;

    @Value("${video.download.interrupt-retry-backoff-ms:1200}")
    private long downloadInterruptRetryBackoffMs;

    private boolean postCompletionPersonaArtifactsEnabled = false;

    private ExecutorService workerPool;
    private ScheduledExecutorService watchdogScheduler;
    private Semaphore downloadSemaphore;
    private Semaphore transcribeSemaphore;
    private Semaphore phase2Semaphore;
    private volatile boolean running = true;
    private Thread dispatcherThread;

    @PostConstruct
    public void start() {
        int workerConcurrency = Math.max(1, configuredWorkerConcurrency);
        int downloadConcurrency = Math.max(1, configuredDownloadConcurrency);
        int transcribeConcurrency = Math.max(1, configuredTranscribeConcurrency);
        int phase2Concurrency = Math.max(1, configuredPhase2Concurrency);
        downloadSemaphore = new Semaphore(downloadConcurrency);
        transcribeSemaphore = new Semaphore(transcribeConcurrency);
        phase2Semaphore = new Semaphore(phase2Concurrency);
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
                "TaskProcessingWorker started with workerConcurrency={} (configured={}), "
                        + "downloadConcurrency={} (configured={}), transcribeConcurrency={} (configured={}), "
                        + "phase2Concurrency={} (configured={}), legacyIoConcurrency={} (configured={})",
                workerConcurrency,
                configuredWorkerConcurrency,
                downloadConcurrency,
                configuredDownloadConcurrency,
                transcribeConcurrency,
                configuredTranscribeConcurrency,
                phase2Concurrency,
                configuredPhase2Concurrency,
                Math.max(1, configuredIoConcurrency),
                configuredIoConcurrency
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
                if (loadScheduler != null && loadScheduler.shouldPauseDispatch()) {
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
        TaskDeduplicationService.NormalizedTaskInput normalizedTaskInput = null;
        boolean activeKeyClaimed = false;
        try {
            if (taskQueueManager.isTaskCancelled(task.taskId)) {
                finalizeCancelled(task, "任务已取消");
                return;
            }

            normalizedTaskInput = taskDeduplicationService.normalizeTaskInput(task);
            taskQueueManager.updateNormalizedVideoInput(
                    task.taskId,
                    normalizedTaskInput.normalizedVideoUrl,
                    normalizedTaskInput.normalizedVideoKey
            );
            String activeOwnerTaskId = taskDeduplicationService.registerOrGetActiveOwner(
                    normalizedTaskInput.normalizedVideoKey,
                    task.taskId
            );
            activeKeyClaimed = task.taskId.equals(activeOwnerTaskId);
            if (!activeKeyClaimed) {
                handleDedupedTask(task, normalizedTaskInput, activeOwnerTaskId, "active task duplicate");
                return;
            }
            String persistedDuplicateTaskId = taskDeduplicationService.findReusablePersistedTask(
                    normalizedTaskInput.normalizedVideoKey,
                    task.taskId
            ).map(record -> record.taskId).orElse("");
            if (!persistedDuplicateTaskId.isBlank()) {
                handleDedupedTask(task, normalizedTaskInput, persistedDuplicateTaskId, "persisted task duplicate");
                return;
            }

            String outputDir = task.outputDir != null ? task.outputDir : "./output/" + task.taskId;
            TaskWatchdog watchdog = taskWatchdogFactory.create(task.taskId);
            webSocketHandler.broadcastTaskUpdate(task);

            if (!prepareProbeStage(task)) {
                return;
            }

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
                boolean progressApplied = taskQueueManager.updateProgress(taskId, progress, outwardMessage);
                if (progressApplied) {
                    webSocketHandler.broadcastTaskUpdate(taskId, "PROCESSING", progress, outwardMessage, null);
                }
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
            TaskTransitionResult completion = taskQueueManager.completeTask(task.taskId, result.markdownPath);
            if (!completion.isApplied()) {
                if (completion.currentStatus == TaskQueueManager.TaskStatus.CANCELLED) {
                    finalizeCancelled(task, "任务已取消");
                    return;
                }
                logger.info(
                        "Skip completion side effects because COMPLETE transition was not applied: taskId={} outcome={} reason={}",
                        task.taskId,
                        completion.outcome,
                        completion.reason
                );
                return;
            }
            TaskEntry completedTask = taskQueueManager.getTask(task.taskId);
            if (completedTask != null) {
                webSocketHandler.broadcastTaskUpdate(completedTask);
                webSocketHandler.broadcastTaskTerminalEvent(completedTask);
            } else {
                webSocketHandler.broadcastTaskUpdate(task.taskId, "COMPLETED", 1.0, "处理完成", result.markdownPath);
            }
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
            TaskTransitionResult failure = taskQueueManager.failTask(task.taskId, rawError);
            if (failure.isApplied()) {
                TaskEntry refreshedTask = taskQueueManager.getTask(task.taskId);
                if (refreshedTask != null) {
                    webSocketHandler.broadcastTaskUpdate(refreshedTask);
                    webSocketHandler.broadcastTaskTerminalEvent(refreshedTask);
                } else {
                    String status = failure.currentStatus != null ? failure.currentStatus.name() : "FAILED";
                    webSocketHandler.broadcastTaskUpdate(task.taskId, status, task.progress, userMessage, null);
                }
            } else if (failure.currentStatus == TaskQueueManager.TaskStatus.CANCELLED) {
                finalizeCancelled(task, "任务已取消");
            } else {
                logger.info(
                        "Skip failure broadcast because FAIL transition was not applied: taskId={} outcome={} reason={}",
                        task.taskId,
                        failure.outcome,
                        failure.reason
                );
            }
        } finally {
            if (activeKeyClaimed && normalizedTaskInput != null) {
                taskDeduplicationService.releaseActiveOwner(
                        normalizedTaskInput.normalizedVideoKey,
                        task.taskId
                );
            }
            orchestrator.clearProgressCallback(task.taskId);
        }
    }

    private boolean prepareProbeStage(TaskEntry task) {
        TaskProbeService.ProbeOutcome cachedProbeOutcome = resolveCachedProbeOutcome(task);
        if (cachedProbeOutcome != null) {
            logger.info("Reuse cached probe payload and skip fresh probe: taskId={} videoUrl={}", task.taskId, task.videoUrl);
            return applyForegroundProbeOutcome(task, cachedProbeOutcome);
        }
        if (shouldProbeInBackground(task)) {
            return detachProbeFromCriticalPath(task);
        }
        TaskProbeService.ProbeOutcome probeOutcome = taskProbeService.probeTask(task);
        return applyForegroundProbeOutcome(task, probeOutcome);
    }

    private boolean shouldProbeInBackground(TaskEntry task) {
        if (task == null || !isHttpUrl(task.videoUrl)) {
            return false;
        }
        VideoProcessingOrchestrator.BookProcessingOptions bookOptions = buildBookProcessingOptions(task);
        return !orchestrator.shouldRunBookPipeline(task.videoUrl, bookOptions);
    }

    private boolean detachProbeFromCriticalPath(TaskEntry task) {
        TaskTransitionResult probeDetached = taskQueueManager.markProbeFinished(
                task.taskId,
                "探测转后台执行，开始下载",
                0.02,
                Map.of(),
                null
        );
        if (!probeDetached.isApplied() && probeDetached.currentStatus == TaskQueueManager.TaskStatus.CANCELLED) {
            finalizeCancelled(task, "任务已取消，处理已停止");
            return false;
        }
        if (probeDetached.isRejected()) {
            throw new IllegalStateException(
                    "Failed to detach probe from critical path: taskId="
                            + task.taskId
                            + ", outcome="
                            + probeDetached.outcome
                            + ", reason="
                            + probeDetached.reason
            );
        }
        TaskEntry processingTask = taskQueueManager.getTask(task.taskId);
        if (processingTask != null) {
            webSocketHandler.broadcastTaskUpdate(processingTask);
        }
        // 这里把探测降级为旁路元数据补全，避免远端视频的探测阻塞下载起跑。
        CompletableFuture
                .supplyAsync(() -> taskProbeService.probeTask(task))
                .thenAccept(probeOutcome -> applyBackgroundProbeOutcome(task, probeOutcome))
                .exceptionally(error -> {
                    logger.warn(
                            "Background probe crashed, keep pipeline running: taskId={} videoUrl={} err={}",
                            task.taskId,
                            task.videoUrl,
                            extractThrowableMessage(error)
                    );
                    return null;
                });
        return true;
    }

    private boolean applyForegroundProbeOutcome(TaskEntry task, TaskProbeService.ProbeOutcome probeOutcome) {
        if (probeOutcome == null || !probeOutcome.success) {
            throw new RuntimeException(firstNonBlank(probeOutcome != null ? probeOutcome.errorMessage : null, "Task probe failed"));
        }
        mergeProbeOutcomeIntoTask(task, probeOutcome);
        TaskTransitionResult probeFinished = taskQueueManager.markProbeFinished(
                task.taskId,
                firstNonBlank(probeOutcome.statusMessage, "探测完成，开始处理"),
                0.08,
                probeOutcome.payload,
                probeOutcome.preferredTitle
        );
        if (!probeFinished.isApplied() && probeFinished.currentStatus == TaskQueueManager.TaskStatus.CANCELLED) {
            finalizeCancelled(task, "任务已取消，处理已停止");
            return false;
        }
        if (probeFinished.isRejected()) {
            throw new IllegalStateException(
                    "Probe transition rejected: taskId="
                            + task.taskId
                            + ", outcome="
                            + probeFinished.outcome
                            + ", reason="
                            + probeFinished.reason
            );
        }
        TaskEntry probeSyncedTask = taskQueueManager.getTask(task.taskId);
        if (probeSyncedTask != null) {
            webSocketHandler.broadcastTaskUpdate(probeSyncedTask);
        }
        return true;
    }

    private void applyBackgroundProbeOutcome(TaskEntry task, TaskProbeService.ProbeOutcome probeOutcome) {
        if (probeOutcome == null || !probeOutcome.success) {
            logger.warn(
                    "Background probe failed, keep pipeline running: taskId={} videoUrl={} err={}",
                    task != null ? task.taskId : "",
                    task != null ? task.videoUrl : "",
                    firstNonBlank(probeOutcome != null ? probeOutcome.errorMessage : null, "probe returned unsuccessful result")
            );
            return;
        }
        TaskEntry currentTask = taskQueueManager.getTask(task.taskId);
        if (currentTask == null || currentTask.status == TaskQueueManager.TaskStatus.CANCELLED) {
            return;
        }
        boolean probeChanged = mergeProbeOutcomeIntoTask(task, probeOutcome);
        if (!probeChanged) {
            return;
        }
        TaskEntry refreshedTask = taskQueueManager.getTask(task.taskId);
        if (refreshedTask != null) {
            webSocketHandler.broadcastTaskUpdate(refreshedTask);
        }
    }

    private boolean mergeProbeOutcomeIntoTask(TaskEntry task, TaskProbeService.ProbeOutcome probeOutcome) {
        if (task == null || probeOutcome == null) {
            return false;
        }
        boolean titleUpdated = false;
        if (probeOutcome.preferredTitle != null && !probeOutcome.preferredTitle.isBlank()) {
            titleUpdated = taskQueueManager.updateTaskTitle(task.taskId, probeOutcome.preferredTitle);
        }
        boolean payloadUpdated = false;
        if (probeOutcome.payload != null && !probeOutcome.payload.isEmpty()) {
            payloadUpdated = taskQueueManager.updateProbePayload(task.taskId, probeOutcome.payload);
            webSocketHandler.broadcastTaskProbeResult(task.taskId, task.userId, probeOutcome.payload);
        }
        return titleUpdated || payloadUpdated;
    }

    // probe_payload 只会在成功探测后落库或合并，因此重试命中非空 payload 时可直接复用。
    // 这样能避免人工修复后的重复 GetVideoInfo，同时保持标题和探测元数据与上次成功结果一致。
    private TaskProbeService.ProbeOutcome resolveCachedProbeOutcome(TaskEntry task) {
        if (task == null || task.probePayload == null || task.probePayload.isEmpty()) {
            return null;
        }
        Map<String, Object> cachedPayload = new LinkedHashMap<>(task.probePayload);
        String preferredTitle = firstNonBlank(
                TaskProbeService.formatProbePayloadTitle(cachedPayload),
                task.title
        );
        if (!preferredTitle.isBlank()) {
            cachedPayload.put("title", preferredTitle);
        }
        return TaskProbeService.ProbeOutcome.success(
                preferredTitle,
                "复用缓存探测结果，开始处理",
                cachedPayload
        );
    }

    private void handleDedupedTask(
            TaskEntry task,
            TaskDeduplicationService.NormalizedTaskInput normalizedTaskInput,
            String duplicateOfTaskId,
            String reason
    ) {
        TaskEntry dedupedTask = taskQueueManager.markTaskDeduped(
                task.taskId,
                normalizedTaskInput != null ? normalizedTaskInput.normalizedVideoUrl : task.videoUrl,
                normalizedTaskInput != null ? normalizedTaskInput.normalizedVideoKey : "",
                duplicateOfTaskId,
                "任务已去重"
        );
        if (dedupedTask == null) {
            return;
        }
        webSocketHandler.broadcastTaskDeduped(
                dedupedTask.taskId,
                dedupedTask.userId,
                duplicateOfTaskId,
                normalizedTaskInput != null ? normalizedTaskInput.normalizedVideoKey : "",
                reason
        );
        taskQueueManager.removeTask(dedupedTask.taskId);
    }

    private void triggerPersonaArtifactsAfterCompletion(
            TaskEntry task,
            VideoProcessingOrchestrator.ProcessingResult result
    ) {
        if (!postCompletionPersonaArtifactsEnabled || personaAwareReadingService == null || result == null) {
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
            return executeTaskPipeline(task, outputDir);
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
            boolean interruptOnRestart = decision.action() == TaskWatchdog.Action.RESTART
                    && watchdog.shouldInterruptOnRestart(decision.stage());
            if (decision.action() == TaskWatchdog.Action.RESTART) {
                if (interruptOnRestart) {
                    String message = String.format(
                            "阶段长时间无进展，准备重启子步骤（阶段=%s，重启=%d/%d）",
                            decision.stage(),
                            decision.stageRestartCount(),
                            watchdog.maxRestartPerStage()
                    );
                    logger.warn("[{}] {}", task.taskId, message);
                    webSocketHandler.broadcastTaskUpdate(task.taskId, "PROCESSING", task.progress, message, null);
                } else {
                    logger.info(
                            "[{}] Watchdog idle strike deferred for heartbeat-strong stage: stage={} restart={}/{}",
                            task.taskId,
                            decision.stage(),
                            decision.stageRestartCount(),
                            watchdog.maxRestartPerStage()
                    );
                    return;
                }
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
            int downloadInterruptRetryCount = 0;
            while (true) {
                attempt += 1;
                clearInterruptFlag();
                watchdog.onAttemptStart(attempt);
                decisionRef.set(TaskWatchdog.Decision.none());
                try {
                    VideoProcessingOrchestrator.ProcessingResult result = executeTaskPipeline(task, outputDir);
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

    private VideoProcessingOrchestrator.ProcessingResult executeTaskPipeline(
            TaskEntry task,
            String outputDir
    ) throws Exception {
        VideoProcessingOrchestrator.BookProcessingOptions bookOptions = buildBookProcessingOptions(task);
        if (orchestrator.shouldRunBookPipeline(task.videoUrl, bookOptions)) {
            return orchestrator.processVideo(
                    task.taskId,
                    task.videoUrl,
                    outputDir,
                    bookOptions
            );
        }
        TaskRuntimeRecoveryService.ResumeDecision resumeDecision = resolveVideoResumeDecision(task, outputDir);
        VideoProcessingOrchestrator.IOPhaseResult recoveredIoResult =
                buildRecoveredIoPhaseResult(task, outputDir, resumeDecision);
        String requestedStartStage = firstNonBlank(
                resumeDecision != null ? resumeDecision.resumeFromStage() : "",
                "download"
        );
        String resolvedStartStage = firstNonBlank(
                orchestrator.reconcileRecoveredRuntimeContext(
                        task.taskId,
                        recoveredIoResult,
                        requestedStartStage,
                        recoveredIoResult.timeouts
                ),
                requestedStartStage
        );
        if ("download".equalsIgnoreCase(resolvedStartStage)) {
            return executeVideoPipelineFromDownload(task, firstNonBlank(recoveredIoResult.outputDir, outputDir));
        }
        syncRecoveredOutputDir(task, recoveredIoResult.outputDir);
        syncRecoveredTaskTitle(task, firstNonBlank(
                resumeDecision != null ? resumeDecision.findText("video_title") : "",
                recoveredIoResult.metricsVideoTitle
        ));
        return executeRecoveredVideoPipeline(task, recoveredIoResult, resolvedStartStage);
    }

    private VideoProcessingOrchestrator.ProcessingResult executeVideoPipelineFromDownload(
            TaskEntry task,
            String outputDir
    ) throws Exception {
        VideoProcessingOrchestrator.IOPhaseResult ioResult =
                executeVideoDownloadPhaseWithPermit(task.taskId, task.videoUrl, outputDir);
        syncRecoveredOutputDirAfterDownload(task, ioResult);
        syncRecoveredTitleAfterDownload(task, ioResult);
        ioResult = executeVideoTranscribePhaseWithPermit(task.taskId, ioResult);
        ioResult = orchestrator.processVideoStage1Phase(task.taskId, ioResult);
        return executeVideoPhase2WithPermit(task.taskId, ioResult);
    }

    private VideoProcessingOrchestrator.ProcessingResult executeRecoveredVideoPipeline(
            TaskEntry task,
            VideoProcessingOrchestrator.IOPhaseResult ioResult,
            String startStage
    ) throws Exception {
        ioResult.recoveryStartStage = startStage;
        return switch (startStage) {
            case "transcribe" -> {
                VideoProcessingOrchestrator.IOPhaseResult transcribed =
                        executeVideoTranscribePhaseWithPermit(task.taskId, ioResult);
                VideoProcessingOrchestrator.IOPhaseResult staged =
                        orchestrator.processVideoStage1Phase(task.taskId, transcribed);
                yield executeVideoPhase2WithPermit(task.taskId, staged);
            }
            case "stage1" -> {
                VideoProcessingOrchestrator.IOPhaseResult staged =
                        orchestrator.processVideoStage1Phase(task.taskId, ioResult);
                yield executeVideoPhase2WithPermit(task.taskId, staged);
            }
            case "phase2a" -> executeVideoPhase2WithPermit(task.taskId, ioResult);
            case "asset_extract_java" -> executeVideoAssetExtractStageWithPermit(task.taskId, ioResult);
            case "phase2b" -> executeVideoPhase2BStageWithPermit(task.taskId, ioResult);
            case "completed" -> orchestrator.processVideoFromRecoveredOutputs(task.taskId, ioResult);
            default -> executeVideoPipelineFromDownload(task, firstNonBlank(ioResult.outputDir, task.outputDir));
        };
    }

    private TaskRuntimeRecoveryService.ResumeDecision resolveVideoResumeDecision(TaskEntry task, String outputDir) {
        if (task == null || taskRuntimeRecoveryService == null) {
            return null;
        }
        try {
            return taskRuntimeRecoveryService.resolveResumeDecision(
                    task.videoUrl,
                    firstNonBlank(task.outputDir, outputDir),
                    task.resultPath
            ).orElse(null);
        } catch (Exception error) {
            logger.warn(
                    "Resolve runtime resume decision failed: taskId={} outputDir={} err={}",
                    task.taskId,
                    outputDir,
                    error.getMessage()
            );
            return null;
        }
    }

    private VideoProcessingOrchestrator.IOPhaseResult buildRecoveredIoPhaseResult(
            TaskEntry task,
            String outputDir,
            TaskRuntimeRecoveryService.ResumeDecision resumeDecision
    ) {
        VideoProcessingOrchestrator.IOPhaseResult ioResult = new VideoProcessingOrchestrator.IOPhaseResult();
        ioResult.taskId = task.taskId;
        ioResult.videoUrl = task.videoUrl;
        ioResult.outputDir = firstNonBlank(
                resumeDecision != null ? resumeDecision.findText("output_dir") : "",
                task.outputDir,
                outputDir
        );
        ioResult.videoPath = firstNonBlank(
                resumeDecision != null ? resumeDecision.findText("video_path") : "",
                task.videoUrl
        );
        ioResult.videoDuration = Math.max(
                1.0d,
                resumeDecision != null ? resumeDecision.findDouble("duration_sec", "video_duration") : 0.0d
        );
        ioResult.downloadedFromUrl = isHttpUrl(task.videoUrl);
        ioResult.cleanupSourcePath = ioResult.downloadedFromUrl ? ioResult.videoPath : "";
        ioResult.pipelineStartTimeMs = System.currentTimeMillis();
        ioResult.metricsOutputDir = ioResult.outputDir;
        ioResult.metricsVideoPath = ioResult.videoPath;
        ioResult.metricsInputVideoUrl = task.videoUrl;
        ioResult.metricsVideoTitle = resumeDecision != null ? resumeDecision.findText("video_title") : "";
        ioResult.recoveryStartStage = resumeDecision != null ? resumeDecision.resumeFromStage() : "download";
        ioResult.subtitlePath = resumeDecision != null ? resumeDecision.findText("subtitle_path") : "";
        ioResult.phase2aSemanticUnitsPath = resumeDecision != null
                ? resumeDecision.findText("semantic_units_path", "phase2a_semantic_units_path")
                : "";

        PythonGrpcClient.DownloadResult downloadResult = new PythonGrpcClient.DownloadResult();
        downloadResult.success = true;
        downloadResult.videoPath = ioResult.videoPath;
        downloadResult.durationSec = ioResult.videoDuration;
        downloadResult.videoTitle = ioResult.metricsVideoTitle;
        downloadResult.resolvedUrl = firstNonBlank(
                resumeDecision != null ? resumeDecision.findText("resolved_url") : "",
                task.videoUrl
        );
        downloadResult.sourcePlatform = resumeDecision != null ? resumeDecision.findText("source_platform") : "";
        downloadResult.canonicalId = resumeDecision != null ? resumeDecision.findText("canonical_id") : "";
        downloadResult.contentType = resumeDecision != null ? resumeDecision.findText("content_type") : "";
        ioResult.downloadResult = downloadResult;

        String step2JsonPath = resumeDecision != null ? resumeDecision.findText("step2_json_path") : "";
        String step6JsonPath = resumeDecision != null ? resumeDecision.findText("step6_json_path") : "";
        String sentenceTimestampsPath = resumeDecision != null ? resumeDecision.findText("sentence_timestamps_path") : "";
        String recoveryStartStage = firstNonBlank(ioResult.recoveryStartStage, "download");
        boolean runtimeStage1Ready = "phase2a".equalsIgnoreCase(recoveryStartStage)
                || "asset_extract_java".equalsIgnoreCase(recoveryStartStage)
                || "phase2b".equalsIgnoreCase(recoveryStartStage)
                || "completed".equalsIgnoreCase(recoveryStartStage);
        if (runtimeStage1Ready || !step2JsonPath.isBlank() || !step6JsonPath.isBlank() || !sentenceTimestampsPath.isBlank()) {
            PythonGrpcClient.Stage1Result stage1Result = new PythonGrpcClient.Stage1Result();
            stage1Result.success = true;
            stage1Result.step2JsonPath = step2JsonPath;
            stage1Result.step6JsonPath = step6JsonPath;
            stage1Result.sentenceTimestampsPath = sentenceTimestampsPath;
            ioResult.stage1Result = stage1Result;
        }
        return ioResult;
    }

    private void syncRecoveredOutputDir(TaskEntry task, String resolvedOutputDir) {
        if (task == null || taskQueueManager == null) {
            return;
        }
        String normalizedOutputDir = firstNonBlank(resolvedOutputDir, "");
        if (normalizedOutputDir.isBlank()) {
            return;
        }
        TaskEntry currentTask = taskQueueManager.getTask(task.taskId);
        if (currentTask == null) {
            return;
        }
        if (normalizedOutputDir.equals(firstNonBlank(currentTask.outputDir, ""))) {
            return;
        }
        taskQueueManager.updateTaskOutputDir(task.taskId, normalizedOutputDir);
    }

    private void syncRecoveredTitleAfterDownload(
            TaskEntry task,
            VideoProcessingOrchestrator.IOPhaseResult ioResult
    ) {
        if (task == null || ioResult == null || ioResult.downloadResult == null) {
            return;
        }
        syncRecoveredTaskTitle(task, ioResult.downloadResult.videoTitle);
    }

    private void syncRecoveredOutputDirAfterDownload(
            TaskEntry task,
            VideoProcessingOrchestrator.IOPhaseResult ioResult
    ) {
        if (task == null || ioResult == null || taskQueueManager == null) {
            return;
        }
        String resolvedOutputDir = firstNonBlank(ioResult.outputDir, ioResult.metricsOutputDir);
        if (resolvedOutputDir == null || resolvedOutputDir.isBlank()) {
            return;
        }
        TaskEntry currentTask = taskQueueManager.getTask(task.taskId);
        if (currentTask == null) {
            return;
        }
        String currentOutputDir = firstNonBlank(currentTask.outputDir, "");
        if (resolvedOutputDir.equals(currentOutputDir)) {
            return;
        }
        taskQueueManager.updateTaskOutputDir(task.taskId, resolvedOutputDir);
    }

    private void syncRecoveredTaskTitle(TaskEntry task, String recoveredTitle) {
        if (task == null || taskQueueManager == null) {
            return;
        }
        String normalizedTitle = firstNonBlank(recoveredTitle, "").trim();
        if (normalizedTitle.isBlank()) {
            return;
        }
        TaskEntry currentTask = taskQueueManager.getTask(task.taskId);
        if (currentTask == null) {
            return;
        }
        if (normalizedTitle.equals(firstNonBlank(currentTask.title, ""))) {
            return;
        }
        if (!taskQueueManager.updateTaskTitle(task.taskId, normalizedTitle)) {
            return;
        }
        TaskEntry refreshedTask = taskQueueManager.getTask(task.taskId);
        if (refreshedTask != null && webSocketHandler != null) {
            webSocketHandler.broadcastTaskUpdate(refreshedTask);
        }
    }

    private VideoProcessingOrchestrator.IOPhaseResult executeVideoDownloadPhaseWithPermit(
            String taskId,
            String videoUrl,
            String outputDir
    ) throws Exception {
        Semaphore semaphore = downloadSemaphore;
        boolean permitAcquired = false;
        if (semaphore != null) {
            acquirePhasePermit(taskId, semaphore, "download");
            permitAcquired = true;
        }
        try {
            return orchestrator.processVideoDownloadPhase(taskId, videoUrl, outputDir);
        } finally {
            if (permitAcquired) {
                semaphore.release();
            }
        }
    }

    private VideoProcessingOrchestrator.IOPhaseResult executeVideoTranscribePhaseWithPermit(
            String taskId,
            VideoProcessingOrchestrator.IOPhaseResult ioResult
    ) throws Exception {
        Semaphore semaphore = transcribeSemaphore;
        boolean permitAcquired = false;
        if (semaphore != null) {
            acquirePhasePermit(taskId, semaphore, "transcribe");
            permitAcquired = true;
        }
        try {
            return orchestrator.processVideoTranscribePhase(taskId, ioResult);
        } finally {
            if (permitAcquired) {
                semaphore.release();
            }
        }
    }

    private VideoProcessingOrchestrator.ProcessingResult executeVideoPhase2WithPermit(
            String taskId,
            VideoProcessingOrchestrator.IOPhaseResult ioResult
    ) throws Exception {
        return executeVideoPhase2StageWithPermit(
                taskId,
                "phase2",
                () -> orchestrator.processVideoLLMPhase(taskId, ioResult)
        );
    }

    private VideoProcessingOrchestrator.ProcessingResult executeVideoAssetExtractStageWithPermit(
            String taskId,
            VideoProcessingOrchestrator.IOPhaseResult ioResult
    ) throws Exception {
        return executeVideoPhase2StageWithPermit(
                taskId,
                "asset_extract_java",
                () -> orchestrator.processVideoFromAssetExtractStage(taskId, ioResult)
        );
    }

    private VideoProcessingOrchestrator.ProcessingResult executeVideoPhase2BStageWithPermit(
            String taskId,
            VideoProcessingOrchestrator.IOPhaseResult ioResult
    ) throws Exception {
        return executeVideoPhase2StageWithPermit(
                taskId,
                "phase2b",
                () -> orchestrator.processVideoFromPhase2BStage(taskId, ioResult)
        );
    }

    private VideoProcessingOrchestrator.ProcessingResult executeVideoPhase2StageWithPermit(
            String taskId,
            String phaseName,
            Phase2Execution phase2Execution
    ) throws Exception {
        Semaphore semaphore = phase2Semaphore;
        boolean permitAcquired = false;
        if (semaphore != null) {
            acquirePhasePermit(taskId, semaphore, phaseName);
            permitAcquired = true;
        }
        try {
            return phase2Execution.execute();
        } finally {
            if (permitAcquired) {
                semaphore.release();
            }
        }
    }

    @FunctionalInterface
    private interface Phase2Execution {
        VideoProcessingOrchestrator.ProcessingResult execute() throws Exception;
    }

    private void acquirePhasePermit(String taskId, Semaphore semaphore, String phaseName) throws InterruptedException {
        while (true) {
            if (taskQueueManager != null && taskQueueManager.isTaskCancelled(taskId)) {
                throw new CancellationException("task cancelled while waiting for " + phaseName + " phase permit");
            }
            if (semaphore.tryAcquire(500, TimeUnit.MILLISECONDS)) {
                return;
            }
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
        options.bookTitle = task.bookOptions.bookTitle;
        options.leafTitle = task.bookOptions.leafTitle;
        options.leafOutlineIndex = task.bookOptions.leafOutlineIndex;
        options.storageKey = task.bookOptions.storageKey;
        if ((options.chapterSelector == null || options.chapterSelector.isBlank())
                && (options.sectionSelector == null || options.sectionSelector.isBlank())
                && options.splitByChapter == null
                && options.splitBySection == null
                && options.pageOffset == null
                && (options.bookTitle == null || options.bookTitle.isBlank())
                && (options.leafTitle == null || options.leafTitle.isBlank())
                && (options.leafOutlineIndex == null || options.leafOutlineIndex.isBlank())
                && (options.storageKey == null || options.storageKey.isBlank())) {
            return null;
        }
        return options;
    }

    private void finalizeCancelled(TaskEntry task, String message) {
        TaskTransitionResult result = taskQueueManager.finalizeCancelledTask(task.taskId, message);
        if (result.isApplied()) {
            webSocketHandler.broadcastTaskUpdate(task.taskId, "CANCELLED", task.progress, message, null);
            return;
        }
        logger.info(
                "Skip cancelled broadcast because FINALIZE_CANCELLATION transition was not applied: taskId={} outcome={} reason={}",
                task.taskId,
                result.outcome,
                result.reason
        );
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

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
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
