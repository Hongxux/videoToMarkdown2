package com.mvp.module2.fusion.queue;

import com.mvp.module2.fusion.common.UserFacingErrorMapper;
import com.mvp.module2.fusion.service.TaskStateRepository;
import com.mvp.module2.fusion.service.TaskStateRepository.PersistedTaskRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多用户任务队列管理器。
 * 目标是提供可观测、可取消、可并发限流的任务状态机。
 */
@Component
public class TaskQueueManager {

    private static final Logger logger = LoggerFactory.getLogger(TaskQueueManager.class);

    private final PriorityBlockingQueue<TaskEntry> taskQueue;
    private final Map<String, TaskEntry> allTasks = new HashMap<>();
    private final Map<String, AtomicInteger> userTaskCounts = new HashMap<>();
    private final AtomicLong taskIdGenerator = new AtomicLong(0);
    private final Semaphore processingSlots;
    private final int maxConcurrentTasks;
    private final ExecutorService executorService;
    private volatile boolean persistedTasksRestored;

    @Autowired(required = false)
    private TaskStateRepository taskStateRepository;

    public enum Priority {
        LOW(0),
        NORMAL(1),
        HIGH(2),
        VIP(3);

        private final int value;

        Priority(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public enum TaskStatus {
        QUEUED,
        PROBING,
        PROCESSING,
        COMPLETED,
        FAILED,
        DEDUPED,
        CANCELLED
    }

    public enum TaskEvent {
        START_PROBING,
        FINISH_PROBING,
        COMPLETE,
        FAIL,
        CANCEL,
        FINALIZE_CANCELLATION
    }

    public enum TaskTransitionOutcome {
        APPLIED,
        NO_OP,
        REJECTED
    }

    public static class TaskTransitionResult {
        public final String taskId;
        public final TaskEvent event;
        public final TaskTransitionOutcome outcome;
        public final TaskStatus previousStatus;
        public final TaskStatus currentStatus;
        public final String reason;

        private TaskTransitionResult(
                String taskId,
                TaskEvent event,
                TaskTransitionOutcome outcome,
                TaskStatus previousStatus,
                TaskStatus currentStatus,
                String reason
        ) {
            this.taskId = taskId;
            this.event = event;
            this.outcome = outcome;
            this.previousStatus = previousStatus;
            this.currentStatus = currentStatus;
            this.reason = reason;
        }

        public boolean isApplied() {
            return outcome == TaskTransitionOutcome.APPLIED;
        }

        public boolean isNoOp() {
            return outcome == TaskTransitionOutcome.NO_OP;
        }

        public boolean isRejected() {
            return outcome == TaskTransitionOutcome.REJECTED;
        }

        public boolean isAccepted() {
            return outcome == TaskTransitionOutcome.APPLIED || outcome == TaskTransitionOutcome.NO_OP;
        }
    }

    private static class TransitionDecision {
        private final TaskTransitionOutcome outcome;
        private final TaskStatus nextStatus;
        private final String reason;

        private TransitionDecision(TaskTransitionOutcome outcome, TaskStatus nextStatus, String reason) {
            this.outcome = outcome;
            this.nextStatus = nextStatus;
            this.reason = reason;
        }
    }

    public static class BookProcessingOptions {
        public String chapterSelector;
        public String sectionSelector;
        public Boolean splitByChapter;
        public Boolean splitBySection;
        public Integer pageOffset;
        public String bookTitle;
        public String leafTitle;
        public String leafOutlineIndex;
        public String storageKey;
    }

    public static class TaskEntry implements Comparable<TaskEntry> {
        public String taskId;
        public String userId;
        public String videoUrl;
        public String normalizedVideoKey;
        public String title;
        public String outputDir;
        public BookProcessingOptions bookOptions;
        public Priority priority;
        public TaskStatus status;
        public Instant createdAt;
        public Instant startedAt;
        public Instant completedAt;
        public double progress;
        public String statusMessage;
        public String resultPath;
        public String cleanupSourcePath;
        public String errorMessage;
        public String duplicateOfTaskId;
        public Map<String, Object> probePayload;
        public boolean resourcesReleased;
        public boolean processingSlotAcquired;

        @Override
        public int compareTo(TaskEntry other) {
            int priorityCompare = Integer.compare(other.priority.getValue(), this.priority.getValue());
            if (priorityCompare != 0) {
                return priorityCompare;
            }
            return this.createdAt.compareTo(other.createdAt);
        }
    }

    public TaskQueueManager() {
        this.maxConcurrentTasks = 1;
        this.processingSlots = new Semaphore(this.maxConcurrentTasks);
        this.taskQueue = new PriorityBlockingQueue<>();
        this.executorService = Executors.newFixedThreadPool(this.maxConcurrentTasks);
    }

    @Autowired
    public TaskQueueManager(
            @Value("${task.queue.max-concurrent:1}") int configuredMaxConcurrentTasks
    ) {
        this.maxConcurrentTasks = Math.max(1, configuredMaxConcurrentTasks);
        this.processingSlots = new Semaphore(this.maxConcurrentTasks);
        this.taskQueue = new PriorityBlockingQueue<>();
        this.executorService = Executors.newFixedThreadPool(this.maxConcurrentTasks);
        logger.info(
                "TaskQueueManager initialized with {} concurrent slots (configured={})",
                this.maxConcurrentTasks,
                configuredMaxConcurrentTasks
        );
    }

    @EventListener(ApplicationReadyEvent.class)
    public synchronized void restorePersistedTasks() {
        if (persistedTasksRestored) {
            return;
        }
        if (taskStateRepository == null) {
            return;
        }
        List<PersistedTaskRecord> records = taskStateRepository.listAllTasks();
        if (records.isEmpty()) {
            return;
        }
        int restoredActiveCount = 0;
        int restoredTerminalCount = 0;
        for (PersistedTaskRecord record : records) {
            TaskEntry restored = toTaskEntry(record);
            if (restored == null || restored.taskId == null || restored.taskId.isBlank()) {
                continue;
            }
            if (isActiveStatus(restored.status)) {
                if (allTasks.containsKey(restored.taskId)) {
                    continue;
                }
                if (restored.status == TaskStatus.PROBING || restored.status == TaskStatus.PROCESSING) {
                    restored.status = TaskStatus.QUEUED;
                    restored.startedAt = null;
                    restored.statusMessage = "服务重启后恢复排队，等待重新执行";
                }
                restored.resourcesReleased = false;
                restored.processingSlotAcquired = false;
                taskQueue.offer(restored);
                userTaskCounts.computeIfAbsent(restored.userId, key -> new AtomicInteger(0)).incrementAndGet();
                allTasks.put(restored.taskId, restored);
                restoredActiveCount += 1;
            } else {
                restoredTerminalCount += 1;
            }
        }
        if (restoredActiveCount > 0 || restoredTerminalCount > 0) {
            logger.info("Restored persisted tasks: active={} terminal={}", restoredActiveCount, restoredTerminalCount);
        }
        persistedTasksRestored = true;
    }

    public synchronized TaskEntry submitTask(String userId, String videoUrl, String outputDir, Priority priority) {
        return submitTask(userId, videoUrl, outputDir, priority, null);
    }

    public synchronized TaskEntry submitTask(
            String userId,
            String videoUrl,
            String outputDir,
            Priority priority,
            String preferredTitle
    ) {
        return submitTask(userId, videoUrl, outputDir, priority, preferredTitle, null);
    }

    public synchronized TaskEntry submitTask(
            String userId,
            String videoUrl,
            String outputDir,
            Priority priority,
            String preferredTitle,
            BookProcessingOptions bookOptions
    ) {
        return createTaskEntry(userId, videoUrl, outputDir, priority, preferredTitle, bookOptions);
    }

    public TaskEntry pollNextTask(long timeout, TimeUnit unit) throws InterruptedException {
        if (!processingSlots.tryAcquire(timeout, unit)) {
            return null;
        }
        TaskEntry task = taskQueue.poll(timeout, unit);
        if (task == null) {
            processingSlots.release();
            return null;
        }
        task.processingSlotAcquired = true;
        if (task.status == TaskStatus.CANCELLED) {
            releaseTaskResources(task);
            return null;
        }
        TaskTransitionResult transition = startProbeTask(task);
        if (transition.isApplied()) {
            return task;
        }
        if (task.status == TaskStatus.CANCELLED) {
            releaseTaskResources(task);
        } else {
            releaseProcessingPermit(task);
            logger.warn(
                    "Skip polled task because START_PROBING transition was not applied: taskId={} status={} outcome={} reason={}",
                    task.taskId,
                    task.status,
                    transition.outcome,
                    transition.reason
            );
        }
        return null;
    }

    public synchronized TaskTransitionResult markProbeFinished(
            String taskId,
            String statusMessage,
            double progress,
            Map<String, Object> probePayload,
            String preferredTitle
    ) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return rejectedTransition(taskId, TaskEvent.FINISH_PROBING, null, null, "task not found");
        }
        TransitionDecision decision = decideTransition(task.status, TaskEvent.FINISH_PROBING);
        if (decision.outcome == TaskTransitionOutcome.REJECTED) {
            if (task.status == TaskStatus.CANCELLED) {
                releaseTaskResources(task);
                return noOpTransition(task.taskId, TaskEvent.FINISH_PROBING, task.status, "cancelled task absorbs probe completion");
            }
            return rejectedTransition(task.taskId, TaskEvent.FINISH_PROBING, task.status, task.status, decision.reason);
        }
        if (decision.outcome == TaskTransitionOutcome.NO_OP) {
            return noOpTransition(task.taskId, TaskEvent.FINISH_PROBING, task.status, decision.reason);
        }
        TaskStatus previousStatus = task.status;
        task.status = decision.nextStatus;
        task.progress = Math.max(task.progress, progress);
        task.statusMessage = normalizeOptionalText(statusMessage) != null
                ? normalizeOptionalText(statusMessage)
                : "探测完成，开始处理";
        mergeProbePayload(task, probePayload);
        if (preferredTitle != null && !preferredTitle.isBlank()) {
            task.title = preferredTitle.trim();
        }
        persistTaskState(task);
        return appliedTransition(task.taskId, TaskEvent.FINISH_PROBING, previousStatus, task.status, "probe finished");
    }

    public synchronized TaskTransitionResult completeTask(String taskId, String resultPath) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return rejectedTransition(taskId, TaskEvent.COMPLETE, null, null, "task not found");
        }
        TransitionDecision decision = decideTransition(task.status, TaskEvent.COMPLETE);
        if (decision.outcome == TaskTransitionOutcome.REJECTED) {
            if (task.status == TaskStatus.CANCELLED) {
                logger.info("Skip completion because task already cancelled: {}", taskId);
                releaseTaskResources(task);
                return noOpTransition(task.taskId, TaskEvent.COMPLETE, task.status, "cancelled task absorbs completion");
            }
            return rejectedTransition(task.taskId, TaskEvent.COMPLETE, task.status, task.status, decision.reason);
        }
        if (decision.outcome == TaskTransitionOutcome.NO_OP) {
            return noOpTransition(task.taskId, TaskEvent.COMPLETE, task.status, decision.reason);
        }
        TaskStatus previousStatus = task.status;
        task.status = decision.nextStatus;
        task.completedAt = Instant.now();
        task.progress = 1.0;
        task.statusMessage = "处理完成";
        task.resultPath = resultPath;
        releaseTaskResources(task);
        persistTaskState(task);

        long elapsedMs = task.startedAt != null
                ? task.completedAt.toEpochMilli() - task.startedAt.toEpochMilli()
                : -1L;
        logger.info("Task completed: {} ({}ms)", taskId, elapsedMs);
        return appliedTransition(task.taskId, TaskEvent.COMPLETE, previousStatus, task.status, "task completed");
    }

    public synchronized TaskTransitionResult failTask(String taskId, String errorMessage) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return rejectedTransition(taskId, TaskEvent.FAIL, null, null, "task not found");
        }
        String userMessage = UserFacingErrorMapper.toUserMessage(errorMessage);
        TransitionDecision decision = decideTransition(task.status, TaskEvent.FAIL);
        if (decision.outcome == TaskTransitionOutcome.REJECTED) {
            if (task.status == TaskStatus.CANCELLED) {
                logger.info("Skip failure because task already cancelled: {}", taskId);
                releaseTaskResources(task);
                return noOpTransition(task.taskId, TaskEvent.FAIL, task.status, "cancelled task absorbs failure");
            }
            return rejectedTransition(task.taskId, TaskEvent.FAIL, task.status, task.status, decision.reason);
        }
        if (decision.outcome == TaskTransitionOutcome.NO_OP) {
            return noOpTransition(task.taskId, TaskEvent.FAIL, task.status, decision.reason);
        }
        TaskStatus previousStatus = task.status;
        task.status = decision.nextStatus;
        task.completedAt = Instant.now();
        task.statusMessage = userMessage;
        task.errorMessage = userMessage;
        releaseTaskResources(task);
        persistTaskState(task);

        logger.error("Task failed: {} - rawError={}, userMessage={}", taskId, errorMessage, userMessage);
        return appliedTransition(task.taskId, TaskEvent.FAIL, previousStatus, task.status, "task failed");
    }

    /**
     * 取消策略：
     * 队列中任务立即移除；
     * 处理中任务标记为 CANCELLED，由 worker 在安全点收敛并 finalize。
     */
    public synchronized boolean cancelTask(String taskId) {
        TaskTransitionResult result = cancelTaskTransition(taskId);
        return result.isAccepted() && result.currentStatus == TaskStatus.CANCELLED;
    }

    public synchronized TaskTransitionResult cancelTaskTransition(String taskId) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return rejectedTransition(taskId, TaskEvent.CANCEL, null, null, "task not found");
        }
        TransitionDecision decision = decideTransition(task.status, TaskEvent.CANCEL);
        if (decision.outcome == TaskTransitionOutcome.REJECTED) {
            return rejectedTransition(task.taskId, TaskEvent.CANCEL, task.status, task.status, decision.reason);
        }
        if (decision.outcome == TaskTransitionOutcome.NO_OP) {
            return noOpTransition(task.taskId, TaskEvent.CANCEL, task.status, decision.reason);
        }
        TaskStatus previousStatus = task.status;
        if (previousStatus == TaskStatus.QUEUED) {
            taskQueue.remove(task);
        }
        task.status = decision.nextStatus;
        task.completedAt = Instant.now();
        task.statusMessage = "任务已取消，后续处理已停止";
        task.errorMessage = null;

        if (previousStatus == TaskStatus.QUEUED) {
            releaseTaskResources(task);
        }
        persistTaskState(task);

        logger.info("Task cancelled: {}", taskId);
        return appliedTransition(task.taskId, TaskEvent.CANCEL, previousStatus, task.status, "task cancelled");
    }

    public synchronized boolean removeTask(String taskId) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return false;
        }
        if (task.status == TaskStatus.QUEUED
                || task.status == TaskStatus.PROBING
                || task.status == TaskStatus.PROCESSING) {
            return false;
        }
        taskQueue.remove(task);
        releaseTaskResources(task);
        allTasks.remove(taskId);
        logger.info("Task removed from runtime map: {}", taskId);
        return true;
    }

    public synchronized TaskTransitionResult finalizeCancelledTask(String taskId) {
        return finalizeCancelledTask(taskId, "任务已取消，处理已停止");
    }

    public synchronized TaskTransitionResult finalizeCancelledTask(String taskId, String finalMessage) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return rejectedTransition(taskId, TaskEvent.FINALIZE_CANCELLATION, null, null, "task not found");
        }
        TransitionDecision decision = decideTransition(task.status, TaskEvent.FINALIZE_CANCELLATION);
        if (decision.outcome == TaskTransitionOutcome.REJECTED) {
            return rejectedTransition(task.taskId, TaskEvent.FINALIZE_CANCELLATION, task.status, task.status, decision.reason);
        }
        boolean changed = false;
        if (task.completedAt == null) {
            task.completedAt = Instant.now();
            changed = true;
        }
        String normalizedMessage = normalizeOptionalText(finalMessage);
        String nextMessage = normalizedMessage != null ? normalizedMessage : "任务已取消，处理已停止";
        if (!Objects.equals(task.statusMessage, nextMessage)) {
            task.statusMessage = nextMessage;
            changed = true;
        }
        if (!task.resourcesReleased) {
            releaseTaskResources(task);
            changed = true;
        }
        if (!changed) {
            return noOpTransition(task.taskId, TaskEvent.FINALIZE_CANCELLATION, task.status, "cancellation already finalized");
        }
        persistTaskState(task);
        logger.info("Task cancellation finalized: {}", taskId);
        return appliedTransition(task.taskId, TaskEvent.FINALIZE_CANCELLATION, task.status, task.status, "cancellation finalized");
    }

    public synchronized boolean isTaskCancelled(String taskId) {
        TaskEntry task = allTasks.get(taskId);
        return task != null && task.status == TaskStatus.CANCELLED;
    }

    public synchronized boolean updateProgress(String taskId, double progress, String message) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return false;
        }
        if (task.status != TaskStatus.QUEUED
                && task.status != TaskStatus.PROBING
                && task.status != TaskStatus.PROCESSING) {
            return false;
        }
        task.progress = progress;
        task.statusMessage = message;
        persistTaskState(task);
        return true;
    }

    public synchronized boolean updateTaskTitle(String taskId, String preferredTitle) {
        TaskEntry task = allTasks.get(taskId);
        String normalizedTitle = normalizeOptionalText(preferredTitle);
        if (task == null || normalizedTitle == null) {
            return false;
        }
        if (Objects.equals(task.title, normalizedTitle)) {
            return true;
        }
        task.title = normalizedTitle;
        persistTaskState(task);
        return true;
    }

    public synchronized boolean updateNormalizedVideoInput(String taskId, String normalizedVideoUrl, String normalizedVideoKey) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return false;
        }
        task.videoUrl = normalizeOptionalText(normalizedVideoUrl);
        task.normalizedVideoKey = normalizeOptionalText(normalizedVideoKey);
        persistTaskState(task);
        return true;
    }

    public synchronized boolean updateProbePayload(String taskId, Map<String, Object> probePayload) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return false;
        }
        mergeProbePayload(task, probePayload);
        persistTaskState(task);
        return true;
    }

    public synchronized void updateCleanupSourcePath(String taskId, String cleanupSourcePath) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return;
        }
        task.cleanupSourcePath = normalizeOptionalText(cleanupSourcePath);
        persistTaskState(task);
    }

    public synchronized TaskEntry markTaskDeduped(
            String taskId,
            String normalizedVideoUrl,
            String normalizedVideoKey,
            String duplicateOfTaskId,
            String message
    ) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return null;
        }
        task.videoUrl = normalizeOptionalText(normalizedVideoUrl);
        task.normalizedVideoKey = normalizeOptionalText(normalizedVideoKey);
        task.duplicateOfTaskId = normalizeOptionalText(duplicateOfTaskId);
        task.status = TaskStatus.DEDUPED;
        task.completedAt = Instant.now();
        task.statusMessage = normalizeOptionalText(message) != null ? normalizeOptionalText(message) : "任务已去重";
        task.errorMessage = null;
        releaseTaskResources(task);
        persistTaskState(task);
        return task;
    }

    public synchronized TaskEntry getTask(String taskId) {
        TaskEntry task = allTasks.get(taskId);
        if (task != null || taskStateRepository == null) {
            return task;
        }
        return taskStateRepository.findTask(taskId).map(this::toTaskEntry).orElse(null);
    }

    public synchronized List<TaskEntry> getUserTasks(String userId) {
        List<TaskEntry> tasks = new ArrayList<>();
        for (TaskEntry task : allTasks.values()) {
            if (userId.equals(task.userId)) {
                tasks.add(task);
            }
        }
        tasks.sort((a, b) -> b.createdAt.compareTo(a.createdAt));
        return tasks;
    }

    public synchronized List<TaskEntry> getAllTasks() {
        List<TaskEntry> tasks = new ArrayList<>(allTasks.values());
        tasks.sort((a, b) -> b.createdAt.compareTo(a.createdAt));
        return tasks;
    }

    public synchronized Map<String, Object> getQueueStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("queueSize", taskQueue.size());
        stats.put("processingCount", maxConcurrentTasks - processingSlots.availablePermits());
        stats.put("maxConcurrent", maxConcurrentTasks);
        stats.put("totalTasks", allTasks.size());

        Map<TaskStatus, Long> statusCounts = new HashMap<>();
        for (TaskEntry task : allTasks.values()) {
            statusCounts.merge(task.status, 1L, Long::sum);
        }
        stats.put("statusDistribution", statusCounts);
        return stats;
    }

    public synchronized int cleanupExpiredTasks(long maxAgeHours) {
        Instant cutoff = Instant.now().minusSeconds(maxAgeHours * 3600);
        int removed = 0;
        Iterator<Map.Entry<String, TaskEntry>> iterator = allTasks.entrySet().iterator();
        while (iterator.hasNext()) {
            TaskEntry task = iterator.next().getValue();
            if (task.completedAt != null && task.completedAt.isBefore(cutoff)) {
                iterator.remove();
                removed += 1;
            }
        }
        if (removed > 0) {
            logger.info("Cleaned up {} expired tasks", removed);
        }
        return removed;
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException interruptedError) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        }
    }

    private TaskTransitionResult startProbeTask(TaskEntry task) {
        if (task == null) {
            return rejectedTransition("", TaskEvent.START_PROBING, null, null, "task not found");
        }
        TransitionDecision decision = decideTransition(task.status, TaskEvent.START_PROBING);
        if (decision.outcome == TaskTransitionOutcome.REJECTED) {
            return rejectedTransition(task.taskId, TaskEvent.START_PROBING, task.status, task.status, decision.reason);
        }
        if (decision.outcome == TaskTransitionOutcome.NO_OP) {
            return noOpTransition(task.taskId, TaskEvent.START_PROBING, task.status, decision.reason);
        }
        TaskStatus previousStatus = task.status;
        task.status = decision.nextStatus;
        if (task.startedAt == null) {
            task.startedAt = Instant.now();
        }
        task.statusMessage = "正在探测任务输入";
        persistTaskState(task);
        return appliedTransition(task.taskId, TaskEvent.START_PROBING, previousStatus, task.status, "task started probing");
    }

    private TransitionDecision decideTransition(TaskStatus currentStatus, TaskEvent event) {
        if (event == TaskEvent.START_PROBING) {
            if (currentStatus == TaskStatus.QUEUED) {
                return new TransitionDecision(TaskTransitionOutcome.APPLIED, TaskStatus.PROBING, "queued -> probing");
            }
            if (currentStatus == TaskStatus.PROBING) {
                return new TransitionDecision(TaskTransitionOutcome.NO_OP, TaskStatus.PROBING, "task already probing");
            }
            return new TransitionDecision(TaskTransitionOutcome.REJECTED, currentStatus, "only queued task can start probing");
        }
        if (event == TaskEvent.FINISH_PROBING) {
            if (currentStatus == TaskStatus.PROBING) {
                return new TransitionDecision(TaskTransitionOutcome.APPLIED, TaskStatus.PROCESSING, "probing -> processing");
            }
            if (currentStatus == TaskStatus.PROCESSING) {
                return new TransitionDecision(TaskTransitionOutcome.NO_OP, TaskStatus.PROCESSING, "task already processing");
            }
            return new TransitionDecision(TaskTransitionOutcome.REJECTED, currentStatus, "only probing task can finish probing");
        }
        if (event == TaskEvent.COMPLETE) {
            if (currentStatus == TaskStatus.PROCESSING) {
                return new TransitionDecision(TaskTransitionOutcome.APPLIED, TaskStatus.COMPLETED, "processing -> completed");
            }
            if (currentStatus == TaskStatus.COMPLETED) {
                return new TransitionDecision(TaskTransitionOutcome.NO_OP, TaskStatus.COMPLETED, "task already completed");
            }
            return new TransitionDecision(TaskTransitionOutcome.REJECTED, currentStatus, "only processing task can complete");
        }
        if (event == TaskEvent.FAIL) {
            if (currentStatus == TaskStatus.PROBING || currentStatus == TaskStatus.PROCESSING) {
                return new TransitionDecision(TaskTransitionOutcome.APPLIED, TaskStatus.FAILED, "active task -> failed");
            }
            if (currentStatus == TaskStatus.FAILED) {
                return new TransitionDecision(TaskTransitionOutcome.NO_OP, TaskStatus.FAILED, "task already failed");
            }
            return new TransitionDecision(TaskTransitionOutcome.REJECTED, currentStatus, "only probing or processing task can fail");
        }
        if (event == TaskEvent.CANCEL) {
            if (currentStatus == TaskStatus.QUEUED
                    || currentStatus == TaskStatus.PROBING
                    || currentStatus == TaskStatus.PROCESSING) {
                return new TransitionDecision(TaskTransitionOutcome.APPLIED, TaskStatus.CANCELLED, "active task -> cancelled");
            }
            if (currentStatus == TaskStatus.CANCELLED) {
                return new TransitionDecision(TaskTransitionOutcome.NO_OP, TaskStatus.CANCELLED, "task already cancelled");
            }
            return new TransitionDecision(TaskTransitionOutcome.REJECTED, currentStatus, "only queued, probing or processing task can cancel");
        }
        if (event == TaskEvent.FINALIZE_CANCELLATION) {
            if (currentStatus == TaskStatus.CANCELLED) {
                return new TransitionDecision(TaskTransitionOutcome.APPLIED, TaskStatus.CANCELLED, "finalize cancellation");
            }
            return new TransitionDecision(TaskTransitionOutcome.REJECTED, currentStatus, "only cancelled task can finalize cancellation");
        }
        return new TransitionDecision(TaskTransitionOutcome.REJECTED, currentStatus, "unsupported task transition event");
    }

    private TaskTransitionResult appliedTransition(
            String taskId,
            TaskEvent event,
            TaskStatus previousStatus,
            TaskStatus currentStatus,
            String reason
    ) {
        return new TaskTransitionResult(taskId, event, TaskTransitionOutcome.APPLIED, previousStatus, currentStatus, reason);
    }

    private TaskTransitionResult noOpTransition(
            String taskId,
            TaskEvent event,
            TaskStatus currentStatus,
            String reason
    ) {
        return new TaskTransitionResult(taskId, event, TaskTransitionOutcome.NO_OP, currentStatus, currentStatus, reason);
    }

    private TaskTransitionResult rejectedTransition(
            String taskId,
            TaskEvent event,
            TaskStatus previousStatus,
            TaskStatus currentStatus,
            String reason
    ) {
        return new TaskTransitionResult(taskId, event, TaskTransitionOutcome.REJECTED, previousStatus, currentStatus, reason);
    }

    private String generateTaskId() {
        return String.format("VT_%d_%d", System.currentTimeMillis(), taskIdGenerator.incrementAndGet());
    }

    private void decrementUserTaskCount(String userId) {
        AtomicInteger count = userTaskCounts.get(userId);
        if (count == null) {
            return;
        }
        int updated = count.decrementAndGet();
        if (updated <= 0) {
            userTaskCounts.remove(userId);
        }
    }

    private void releaseProcessingPermit(TaskEntry task) {
        if (task == null || !task.processingSlotAcquired) {
            return;
        }
        task.processingSlotAcquired = false;
        processingSlots.release();
    }

    private void releaseTaskResources(TaskEntry task) {
        if (task == null || task.resourcesReleased) {
            return;
        }
        task.resourcesReleased = true;
        releaseProcessingPermit(task);
        decrementUserTaskCount(task.userId);
    }

    private String normalizeOptionalText(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private TaskEntry createTaskEntry(
            String userId,
            String videoUrl,
            String outputDir,
            Priority priority,
            String preferredTitle,
            BookProcessingOptions bookOptions
    ) {
        String taskId = generateTaskId();

        TaskEntry entry = new TaskEntry();
        entry.taskId = taskId;
        entry.userId = userId;
        entry.videoUrl = videoUrl;
        entry.normalizedVideoKey = null;
        entry.title = normalizeOptionalText(preferredTitle);
        entry.outputDir = outputDir;
        entry.priority = priority;
        entry.status = TaskStatus.QUEUED;
        entry.createdAt = Instant.now();
        entry.progress = 0.0;
        entry.statusMessage = "排队中";
        entry.resourcesReleased = false;
        entry.processingSlotAcquired = false;
        entry.bookOptions = normalizeBookOptions(bookOptions);
        entry.duplicateOfTaskId = null;
        entry.probePayload = null;

        persistAcceptedTaskState(entry);
        allTasks.put(taskId, entry);
        taskQueue.offer(entry);
        userTaskCounts.computeIfAbsent(userId, key -> new AtomicInteger(0)).incrementAndGet();

        logger.info("Task submitted: {} by user {} (priority={})", taskId, userId, priority);
        return entry;
    }

    private BookProcessingOptions normalizeBookOptions(BookProcessingOptions rawOptions) {
        if (rawOptions == null) {
            return null;
        }
        BookProcessingOptions normalized = new BookProcessingOptions();
        normalized.chapterSelector = normalizeOptionalText(rawOptions.chapterSelector);
        normalized.sectionSelector = normalizeOptionalText(rawOptions.sectionSelector);
        normalized.splitByChapter = rawOptions.splitByChapter;
        normalized.splitBySection = rawOptions.splitBySection;
        normalized.pageOffset = rawOptions.pageOffset;
        normalized.bookTitle = normalizeOptionalText(rawOptions.bookTitle);
        normalized.leafTitle = normalizeOptionalText(rawOptions.leafTitle);
        normalized.leafOutlineIndex = normalizeOptionalText(rawOptions.leafOutlineIndex);
        normalized.storageKey = normalizeOptionalText(rawOptions.storageKey);
        if (normalized.chapterSelector == null
                && normalized.sectionSelector == null
                && normalized.splitByChapter == null
                && normalized.splitBySection == null
                && normalized.pageOffset == null
                && normalized.bookTitle == null
                && normalized.leafTitle == null
                && normalized.leafOutlineIndex == null
                && normalized.storageKey == null) {
            return null;
        }
        return normalized;
    }

    private void mergeProbePayload(TaskEntry task, Map<String, Object> probePayload) {
        if (task == null || probePayload == null || probePayload.isEmpty()) {
            return;
        }
        Map<String, Object> merged = new LinkedHashMap<>();
        if (task.probePayload != null && !task.probePayload.isEmpty()) {
            merged.putAll(task.probePayload);
        }
        merged.putAll(probePayload);
        task.probePayload = merged;
    }

    private boolean isActiveStatus(TaskStatus status) {
        return status == TaskStatus.QUEUED
                || status == TaskStatus.PROBING
                || status == TaskStatus.PROCESSING;
    }

    private Priority parsePriority(String rawPriority) {
        String normalized = rawPriority != null ? rawPriority.trim().toUpperCase(Locale.ROOT) : "";
        if (normalized.isEmpty()) {
            return Priority.NORMAL;
        }
        try {
            return Priority.valueOf(normalized);
        } catch (Exception error) {
            return Priority.NORMAL;
        }
    }

    private TaskStatus parseTaskStatus(String rawStatus) {
        String normalized = rawStatus != null ? rawStatus.trim().toUpperCase(Locale.ROOT) : "";
        if (normalized.isEmpty()) {
            return TaskStatus.QUEUED;
        }
        try {
            return TaskStatus.valueOf(normalized);
        } catch (Exception error) {
            return TaskStatus.QUEUED;
        }
    }

    private TaskEntry toTaskEntry(PersistedTaskRecord record) {
        if (record == null) {
            return null;
        }
        TaskEntry task = new TaskEntry();
        task.taskId = normalizeOptionalText(record.taskId);
        task.userId = normalizeOptionalText(record.userId);
        task.videoUrl = normalizeOptionalText(record.videoUrl);
        task.normalizedVideoKey = normalizeOptionalText(record.normalizedVideoKey);
        task.title = normalizeOptionalText(record.title);
        task.outputDir = normalizeOptionalText(record.outputDir);
        task.priority = parsePriority(record.priority);
        task.status = parseTaskStatus(record.status);
        task.createdAt = record.createdAt != null ? record.createdAt : Instant.now();
        task.startedAt = record.startedAt;
        task.completedAt = record.completedAt;
        task.progress = record.progress;
        task.statusMessage = normalizeOptionalText(record.statusMessage);
        task.resultPath = normalizeOptionalText(record.resultPath);
        task.cleanupSourcePath = normalizeOptionalText(record.cleanupSourcePath);
        task.errorMessage = normalizeOptionalText(record.errorMessage);
        task.duplicateOfTaskId = normalizeOptionalText(record.duplicateOfTaskId);
        task.bookOptions = normalizeBookOptions(record.bookOptions);
        task.probePayload = record.probePayload != null && !record.probePayload.isEmpty()
                ? new LinkedHashMap<>(record.probePayload)
                : null;
        task.resourcesReleased = !isActiveStatus(task.status);
        task.processingSlotAcquired = false;
        return task;
    }

    private void persistTaskState(TaskEntry task) {
        if (task == null || taskStateRepository == null) {
            return;
        }
        try {
            taskStateRepository.upsertTask(task);
        } catch (Exception error) {
            logger.error("Persist task state failed: taskId={} err={}", task.taskId, error.getMessage(), error);
        }
    }

    private void persistAcceptedTaskState(TaskEntry task) {
        if (task == null) {
            return;
        }
        if (taskStateRepository == null) {
            throw new IllegalStateException("task state repository unavailable");
        }
        try {
            taskStateRepository.upsertTask(task);
        } catch (Exception error) {
            logger.error(
                "Persist accepted task failed before enqueue: taskId={} userId={} videoUrl={} err={}",
                    task.taskId,
                    task.userId,
                    task.videoUrl,
                    error.getMessage(),
                    error
            );
            throw new IllegalStateException("persist accepted task failed", error);
        }
    }
}
