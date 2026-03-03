package com.mvp.module2.fusion.queue;

import com.mvp.module2.fusion.common.UserFacingErrorMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
        PROCESSING,
        COMPLETED,
        FAILED,
        CANCELLED
    }

    public static class BookProcessingOptions {
        public String chapterSelector;
        public String sectionSelector;
        public Boolean splitByChapter;
        public Boolean splitBySection;
        public Integer pageOffset;
    }

    public static class TaskEntry implements Comparable<TaskEntry> {
        public String taskId;
        public String userId;
        public String videoUrl;
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
        public String errorMessage;
        public boolean resourcesReleased;

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
        this(1);
    }

    @Autowired
    public TaskQueueManager(@Value("${task.queue.max-concurrent:1}") int configuredMaxConcurrentTasks) {
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
        String taskId = generateTaskId();

        TaskEntry entry = new TaskEntry();
        entry.taskId = taskId;
        entry.userId = userId;
        entry.videoUrl = videoUrl;
        entry.title = normalizeOptionalText(preferredTitle);
        entry.outputDir = outputDir;
        entry.priority = priority;
        entry.status = TaskStatus.QUEUED;
        entry.createdAt = Instant.now();
        entry.progress = 0.0;
        entry.statusMessage = "排队中";
        entry.resourcesReleased = false;

        allTasks.put(taskId, entry);
        taskQueue.offer(entry);
        userTaskCounts.computeIfAbsent(userId, key -> new AtomicInteger(0)).incrementAndGet();

        logger.info("Task submitted: {} by user {} (priority={})", taskId, userId, priority);
        return entry;
    }

    public synchronized TaskEntry submitTask(
            String userId,
            String videoUrl,
            String outputDir,
            Priority priority,
            String preferredTitle,
            BookProcessingOptions bookOptions
    ) {
        TaskEntry entry = submitTask(userId, videoUrl, outputDir, priority, preferredTitle);
        entry.bookOptions = normalizeBookOptions(bookOptions);
        return entry;
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
        if (task.status == TaskStatus.CANCELLED) {
            releaseTaskResources(task);
            return null;
        }
        task.status = TaskStatus.PROCESSING;
        task.startedAt = Instant.now();
        task.statusMessage = "处理中";
        return task;
    }

    public synchronized void completeTask(String taskId, String resultPath) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return;
        }
        if (task.status == TaskStatus.CANCELLED) {
            logger.info("Skip completion because task already cancelled: {}", taskId);
            releaseTaskResources(task);
            return;
        }
        task.status = TaskStatus.COMPLETED;
        task.completedAt = Instant.now();
        task.progress = 1.0;
        task.statusMessage = "处理完成";
        task.resultPath = resultPath;
        releaseTaskResources(task);

        long elapsedMs = task.startedAt != null
                ? task.completedAt.toEpochMilli() - task.startedAt.toEpochMilli()
                : -1L;
        logger.info("Task completed: {} ({}ms)", taskId, elapsedMs);
    }

    public synchronized void failTask(String taskId, String errorMessage) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return;
        }
        if (task.status == TaskStatus.CANCELLED) {
            logger.info("Skip failure because task already cancelled: {}", taskId);
            releaseTaskResources(task);
            return;
        }
        String userMessage = UserFacingErrorMapper.toUserMessage(errorMessage);
        task.status = TaskStatus.FAILED;
        task.completedAt = Instant.now();
        task.statusMessage = userMessage;
        task.errorMessage = userMessage;
        releaseTaskResources(task);

        logger.error("Task failed: {} - rawError={}, userMessage={}", taskId, errorMessage, userMessage);
    }

    /**
     * 取消策略：
     * 队列中任务立即移除；
     * 处理中任务标记为 CANCELLED，由 worker 在安全点收敛并 finalize。
     */
    public synchronized boolean cancelTask(String taskId) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null) {
            return false;
        }
        if (task.status != TaskStatus.QUEUED && task.status != TaskStatus.PROCESSING) {
            return false;
        }
        TaskStatus previousStatus = task.status;
        if (previousStatus == TaskStatus.QUEUED) {
            taskQueue.remove(task);
        }
        task.status = TaskStatus.CANCELLED;
        task.completedAt = Instant.now();
        task.statusMessage = "任务已取消，后续步骤已暂停";
        task.errorMessage = null;

        if (previousStatus == TaskStatus.QUEUED) {
            releaseTaskResources(task);
        }

        logger.info("Task cancelled: {}", taskId);
        return true;
    }

    public synchronized void finalizeCancelledTask(String taskId) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null || task.status != TaskStatus.CANCELLED) {
            return;
        }
        if (task.completedAt == null) {
            task.completedAt = Instant.now();
        }
        task.statusMessage = "任务已取消，处理已停止";
        releaseTaskResources(task);
        logger.info("Task cancellation finalized: {}", taskId);
    }

    public synchronized boolean isTaskCancelled(String taskId) {
        TaskEntry task = allTasks.get(taskId);
        return task != null && task.status == TaskStatus.CANCELLED;
    }

    public synchronized void updateProgress(String taskId, double progress, String message) {
        TaskEntry task = allTasks.get(taskId);
        if (task == null || task.status == TaskStatus.CANCELLED) {
            return;
        }
        task.progress = progress;
        task.statusMessage = message;
    }

    public synchronized TaskEntry getTask(String taskId) {
        return allTasks.get(taskId);
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

    private void releaseTaskResources(TaskEntry task) {
        if (task == null || task.resourcesReleased) {
            return;
        }
        task.resourcesReleased = true;
        processingSlots.release();
        decrementUserTaskCount(task.userId);
    }

    private String normalizeOptionalText(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
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
        if (normalized.chapterSelector == null
                && normalized.sectionSelector == null
                && normalized.splitByChapter == null
                && normalized.splitBySection == null
                && normalized.pageOffset == null) {
            return null;
        }
        return normalized;
    }
}
