package com.mvp.module2.fusion.worker;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.service.VideoProcessingOrchestrator;
import com.mvp.module2.fusion.websocket.TaskWebSocketHandler;
import com.mvp.module2.fusion.scheduler.LoadBasedScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.*;

/**
 * 任务处理Worker
 * 
 * 职责：
 * - 从TaskQueueManager取任务
 * - 调用VideoProcessingOrchestrator执行处理
 * - 通过WebSocket推送状态更新
 */
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
    
    private ExecutorService workerPool;
    private volatile boolean running = true;
    private Thread dispatcherThread;
    
    @PostConstruct
    public void start() {
        // 创建worker线程池
        workerPool = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "TaskWorker-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });
        
        // 启动分发线程
        dispatcherThread = new Thread(this::dispatchLoop, "TaskDispatcher");
        dispatcherThread.setDaemon(true);
        dispatcherThread.start();
        
        logger.info("✅ TaskProcessingWorker started");
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
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        logger.info("TaskProcessingWorker stopped");
    }
    
    /**
     * 任务分发循环
     */
    private void dispatchLoop() {
        logger.info("🔄 Task dispatcher loop started");
        
        while (running) {
            try {
                // 检查系统负载
                if (loadScheduler.getSystemState() == LoadBasedScheduler.SystemState.OVERLOADED) {
                    logger.warn("⚠️ System overloaded, pausing task dispatch for 5s");
                    Thread.sleep(5000);
                    continue;
                }
                
                // 从队列获取任务 (阻塞等待)
                TaskEntry task = taskQueueManager.pollNextTask(5, TimeUnit.SECONDS);
                
                if (task != null) {
                    logger.info("📥 Dispatched task: {} ({})", task.taskId, task.priority);
                    
                    // 提交到线程池处理
                    workerPool.submit(() -> processTask(task));
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Dispatcher error", e);
            }
        }
        
        logger.info("Task dispatcher loop stopped");
    }
    
    /**
     * 处理单个任务
     */
    private void processTask(TaskEntry task) {
        logger.info("🎬 Processing task: {}", task.taskId);
        
        try {
            String outputDir = task.outputDir != null ? task.outputDir : "./output/" + task.taskId;
            
            // 注册进度回调
            orchestrator.setProgressCallback((taskId, progress, message) -> {
                taskQueueManager.updateProgress(taskId, progress, message);
                webSocketHandler.broadcastTaskUpdate(taskId, "PROCESSING", progress, message, null);
            });
            
            // 调用编排器执行完整处理流程
            VideoProcessingOrchestrator.ProcessingResult result = 
                orchestrator.processVideo(task.taskId, task.videoUrl, outputDir);
            
            if (result.success) {
                taskQueueManager.completeTask(task.taskId, result.markdownPath);
                webSocketHandler.broadcastTaskUpdate(task.taskId, "COMPLETED", 1.0, "处理完成", result.markdownPath);
                logger.info("✅ Task completed: {} -> {}", task.taskId, result.markdownPath);
            } else {
                throw new RuntimeException(result.errorMessage);
            }
            
        } catch (Exception e) {
            logger.error("❌ Task failed: " + task.taskId, e);
            taskQueueManager.failTask(task.taskId, e.getMessage());
            webSocketHandler.broadcastTaskUpdate(task.taskId, "FAILED", task.progress, e.getMessage(), null);
        }
    }
}
