package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.resilience.ResilientGrpcClient;
import com.mvp.module2.fusion.resilience.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 视频处理 REST API 控制器
 * 
 * 端点：
 * POST /api/tasks          - 提交新任务
 * GET  /api/tasks/{id}     - 获取任务状态
 * GET  /api/tasks/user/{userId} - 获取用户所有任务
 * DELETE /api/tasks/{id}   - 取消任务
 * GET  /api/stats          - 获取队列统计
 * GET  /api/health         - 健康检查
 */
@RestController
@RequestMapping("/api")
// CORS 配置已在 WebConfig 中全局设置
public class VideoProcessingController {
    
    private static final Logger logger = LoggerFactory.getLogger(VideoProcessingController.class);
    
    @Autowired
    private TaskQueueManager taskQueueManager;
    
    @Autowired
    private ResilientGrpcClient grpcClient;
    
    /**
     * 提交新任务
     */
    @PostMapping("/tasks")
    public ResponseEntity<Map<String, Object>> submitTask(@RequestBody TaskSubmitRequest request) {
        logger.info("Received task submission: {} from user {}", request.videoUrl, request.userId);
        
        // 确定优先级
        TaskQueueManager.Priority priority = TaskQueueManager.Priority.NORMAL;
        if ("vip".equalsIgnoreCase(request.priority)) {
            priority = TaskQueueManager.Priority.VIP;
        } else if ("high".equalsIgnoreCase(request.priority)) {
            priority = TaskQueueManager.Priority.HIGH;
        }
        
        // 提交任务
        TaskQueueManager.TaskEntry task = taskQueueManager.submitTask(
            request.userId,
            request.videoUrl,
            request.outputDir,
            priority
        );
        
        return ResponseEntity.ok(Map.of(
            "success", true,
            "taskId", task.taskId,
            "status", task.status.name(),
            "message", "任务已提交，正在排队中"
        ));
    }
    
    /**
     * 获取任务状态
     */
    @GetMapping("/tasks/{taskId}")
    public ResponseEntity<Map<String, Object>> getTask(@PathVariable String taskId) {
        TaskQueueManager.TaskEntry task = taskQueueManager.getTask(taskId);
        
        if (task == null) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok(Map.of(
            "taskId", task.taskId,
            "userId", task.userId,
            "status", task.status.name(),
            "progress", task.progress,
            "statusMessage", task.statusMessage != null ? task.statusMessage : "",
            "resultPath", task.resultPath != null ? task.resultPath : "",
            "errorMessage", task.errorMessage != null ? task.errorMessage : "",
            "createdAt", task.createdAt.toString(),
            "startedAt", task.startedAt != null ? task.startedAt.toString() : "",
            "completedAt", task.completedAt != null ? task.completedAt.toString() : ""
        ));
    }
    
    /**
     * 获取用户的所有任务
     */
    @GetMapping("/tasks/user/{userId}")
    public ResponseEntity<List<TaskQueueManager.TaskEntry>> getUserTasks(@PathVariable String userId) {
        List<TaskQueueManager.TaskEntry> tasks = taskQueueManager.getUserTasks(userId);
        return ResponseEntity.ok(tasks);
    }
    
    /**
     * 取消任务
     */
    @DeleteMapping("/tasks/{taskId}")
    public ResponseEntity<Map<String, Object>> cancelTask(@PathVariable String taskId) {
        boolean cancelled = taskQueueManager.cancelTask(taskId);
        
        if (cancelled) {
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "任务已取消"
            ));
        } else {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "message", "无法取消任务（可能已完成或不存在）"
            ));
        }
    }
    
    /**
     * 获取队列统计
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        Map<String, Object> stats = taskQueueManager.getQueueStats();
        stats.put("circuitBreakerState", grpcClient.getCircuitBreakerState().name());
        stats.put("healthy", grpcClient.isHealthy());
        return ResponseEntity.ok(stats);
    }
    
    /**
     * 健康检查
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        boolean pythonHealthy = grpcClient.isHealthy();
        CircuitBreaker.State cbState = grpcClient.getCircuitBreakerState();
        
        Map<String, Object> health = Map.of(
            "status", pythonHealthy ? "UP" : "DOWN",
            "python", Map.of(
                "healthy", pythonHealthy,
                "circuitBreaker", cbState.name()
            ),
            "queue", taskQueueManager.getQueueStats()
        );
        
        return pythonHealthy 
            ? ResponseEntity.ok(health)
            : ResponseEntity.status(503).body(health);
    }
    
    /**
     * 重置熔断器（管理端点）
     */
    @PostMapping("/admin/reset-circuit-breaker")
    public ResponseEntity<Map<String, Object>> resetCircuitBreaker() {
        grpcClient.resetCircuitBreaker();
        return ResponseEntity.ok(Map.of(
            "success", true,
            "message", "熔断器已重置"
        ));
    }
    
    // ========== 请求体类 ==========
    
    public static class TaskSubmitRequest {
        public String userId;
        public String videoUrl;
        public String outputDir;
        public String priority; // normal, high, vip
    }
}
