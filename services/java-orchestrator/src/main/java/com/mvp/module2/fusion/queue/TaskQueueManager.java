package com.mvp.module2.fusion.queue;

import com.mvp.module2.fusion.common.UserFacingErrorMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 多用户任务队列管理器
 * 
 * 功能：
 * 1. 优先级队列（VIP用户优先）
 * 2. 公平调度（防止单用户独占）
 * 3. 并发控制（限制同时处理任务数）
 * 4. 任务状态跟踪
 */
@Component
public class TaskQueueManager {
    
    private static final Logger logger = LoggerFactory.getLogger(TaskQueueManager.class);
    
    // 优先级队列
    private final PriorityBlockingQueue<TaskEntry> taskQueue;
    
    // 任务状态存储
    private final ConcurrentHashMap<String, TaskEntry> allTasks = new ConcurrentHashMap<>();
    
    // 用户任务计数器（用于公平调度）
    private final ConcurrentHashMap<String, AtomicInteger> userTaskCounts = new ConcurrentHashMap<>();
    
    // 全局任务ID生成器
    private final AtomicLong taskIdGenerator = new AtomicLong(0);
    
    // 并发控制
    private final Semaphore processingSlots;
    private final int maxConcurrentTasks;
    
    // 处理线程池
    private final ExecutorService executorService;
    
    /**
     * 任务优先级
     */
    public enum Priority {
        LOW(0),
        NORMAL(1),
        HIGH(2),
        VIP(3);
        
        private final int value;
        Priority(int value) { this.value = value; }
        public int getValue() { return value; }
    }
    
    /**
     * 任务状态
     */
    public enum TaskStatus {
        QUEUED,         // 排队中
        PROCESSING,     // 处理中
        COMPLETED,      // 已完成
        FAILED,         // 失败
        CANCELLED       // 已取消
    }
    
    /**
     * 任务条目
     */
    public static class TaskEntry implements Comparable<TaskEntry> {
        public String taskId;
        public String userId;
        public String videoUrl;
        public String outputDir;
        public Priority priority;
        public TaskStatus status;
        public Instant createdAt;
        public Instant startedAt;
        public Instant completedAt;
        public double progress;
        public String statusMessage;
        public String resultPath;
        public String errorMessage;
        
        // 用于优先级队列排序
        @Override
        public int compareTo(TaskEntry other) {
            // 1. 优先级高的优先
            int priorityCompare = Integer.compare(other.priority.getValue(), this.priority.getValue());
            if (priorityCompare != 0) return priorityCompare;
            
            // 2. 创建时间早的优先
            return this.createdAt.compareTo(other.createdAt);
        }
    }
    
    public TaskQueueManager() {
        this(4); // 默认最多4个并发任务
    }
    
    public TaskQueueManager(int maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
        this.processingSlots = new Semaphore(maxConcurrentTasks);
        this.taskQueue = new PriorityBlockingQueue<>();
        this.executorService = Executors.newFixedThreadPool(maxConcurrentTasks);
        
        logger.info("TaskQueueManager initialized with {} concurrent slots", maxConcurrentTasks);
    }
    
    /**
     * 提交新任务
     */
    public TaskEntry submitTask(String userId, String videoUrl, String outputDir, Priority priority) {
        String taskId = generateTaskId();
        
        TaskEntry entry = new TaskEntry();
        entry.taskId = taskId;
        entry.userId = userId;
        entry.videoUrl = videoUrl;
        entry.outputDir = outputDir;
        entry.priority = priority;
        entry.status = TaskStatus.QUEUED;
        entry.createdAt = Instant.now();
        entry.progress = 0.0;
        entry.statusMessage = "排队中";
        
        // 记录任务
        allTasks.put(taskId, entry);
        taskQueue.offer(entry);
        
        // 更新用户任务计数
        userTaskCounts.computeIfAbsent(userId, k -> new AtomicInteger(0)).incrementAndGet();
        
        logger.info("Task submitted: {} by user {} (priority={})", taskId, userId, priority);
        
        return entry;
    }
    
    /**
     * 获取下一个待处理任务
     */
    public TaskEntry pollNextTask(long timeout, TimeUnit unit) throws InterruptedException {
        // 获取处理槽
        if (processingSlots.tryAcquire(timeout, unit)) {
            TaskEntry task = taskQueue.poll(timeout, unit);
            if (task != null) {
                task.status = TaskStatus.PROCESSING;
                task.startedAt = Instant.now();
                task.statusMessage = "处理中";
                return task;
            } else {
                processingSlots.release();
            }
        }
        return null;
    }
    
    /**
     * 标记任务完成
     */
    public void completeTask(String taskId, String resultPath) {
        TaskEntry task = allTasks.get(taskId);
        if (task != null) {
            task.status = TaskStatus.COMPLETED;
            task.completedAt = Instant.now();
            task.progress = 1.0;
            task.statusMessage = "处理完成";
            task.resultPath = resultPath;
            
            processingSlots.release();
            decrementUserTaskCount(task.userId);
            
            logger.info("Task completed: {} ({}ms)", taskId, 
                task.completedAt.toEpochMilli() - task.startedAt.toEpochMilli());
        }
    }
    
    /**
     * 标记任务失败
     */
    public void failTask(String taskId, String errorMessage) {
        TaskEntry task = allTasks.get(taskId);
        if (task != null) {
            task.status = TaskStatus.FAILED;
            task.completedAt = Instant.now();
            task.statusMessage = UserFacingErrorMapper.busyMessage();
            task.errorMessage = UserFacingErrorMapper.busyMessage();
            
            processingSlots.release();
            decrementUserTaskCount(task.userId);
            
            logger.error("Task failed: {} - {}", taskId, errorMessage);
        }
    }
    
    /**
     * 取消任务
     */
    public boolean cancelTask(String taskId) {
        TaskEntry task = allTasks.get(taskId);
        if (task != null && (task.status == TaskStatus.QUEUED || task.status == TaskStatus.PROCESSING)) {
            task.status = TaskStatus.CANCELLED;
            task.completedAt = Instant.now();
            task.statusMessage = "已取消";
            
            if (task.status == TaskStatus.PROCESSING) {
                processingSlots.release();
            }
            decrementUserTaskCount(task.userId);
            
            logger.info("Task cancelled: {}", taskId);
            return true;
        }
        return false;
    }
    
    /**
     * 更新任务进度
     */
    public void updateProgress(String taskId, double progress, String message) {
        TaskEntry task = allTasks.get(taskId);
        if (task != null) {
            task.progress = progress;
            task.statusMessage = message;
        }
    }
    
    /**
     * 获取任务状态
     */
    public TaskEntry getTask(String taskId) {
        return allTasks.get(taskId);
    }
    
    /**
     * 获取用户的所有任务
     */
    public List<TaskEntry> getUserTasks(String userId) {
        List<TaskEntry> tasks = new ArrayList<>();
        for (TaskEntry task : allTasks.values()) {
            if (userId.equals(task.userId)) {
                tasks.add(task);
            }
        }
        tasks.sort((a, b) -> b.createdAt.compareTo(a.createdAt));
        return tasks;
    }

    /**
     * 获取所有任务（按创建时间倒序）。
     * 用于移动端任务列表展示，避免前端必须知道 userId。
     */
    public List<TaskEntry> getAllTasks() {
        List<TaskEntry> tasks = new ArrayList<>(allTasks.values());
        tasks.sort((a, b) -> b.createdAt.compareTo(a.createdAt));
        return tasks;
    }
    
    /**
     * 获取队列统计
     */
    public Map<String, Object> getQueueStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("queueSize", taskQueue.size());
        stats.put("processingCount", maxConcurrentTasks - processingSlots.availablePermits());
        stats.put("maxConcurrent", maxConcurrentTasks);
        stats.put("totalTasks", allTasks.size());
        
        // 状态分布
        Map<TaskStatus, Long> statusCounts = new HashMap<>();
        for (TaskEntry task : allTasks.values()) {
            statusCounts.merge(task.status, 1L, Long::sum);
        }
        stats.put("statusDistribution", statusCounts);
        
        return stats;
    }
    
    /**
     * 生成任务ID
     */
    private String generateTaskId() {
        return String.format("VT_%d_%d", 
            System.currentTimeMillis(), 
            taskIdGenerator.incrementAndGet());
    }
    
    /**
     * 减少用户任务计数
     */
    private void decrementUserTaskCount(String userId) {
        AtomicInteger count = userTaskCounts.get(userId);
        if (count != null) {
            count.decrementAndGet();
        }
    }
    
    /**
     * 清理过期任务
     */
    public int cleanupExpiredTasks(long maxAgeHours) {
        Instant cutoff = Instant.now().minusSeconds(maxAgeHours * 3600);
        int removed = 0;
        
        Iterator<Map.Entry<String, TaskEntry>> it = allTasks.entrySet().iterator();
        while (it.hasNext()) {
            TaskEntry task = it.next().getValue();
            if (task.completedAt != null && task.completedAt.isBefore(cutoff)) {
                it.remove();
                removed++;
            }
        }
        
        if (removed > 0) {
            logger.info("Cleaned up {} expired tasks", removed);
        }
        return removed;
    }
    
    /**
     * 关闭队列管理器
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }
}
