package com.videotomd.controller;

import com.videotomd.config.JwtUtil;
import com.videotomd.dto.CreateTaskRequest;
import com.videotomd.entity.Task;
import com.videotomd.service.TaskService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务控制器
 */
@RestController
@RequestMapping("/api/tasks")
@Tag(name = "任务API", description = "视频处理任务相关接口")
public class TaskController {

    private final TaskService taskService;
    private final JwtUtil jwtUtil;

    public TaskController(TaskService taskService, JwtUtil jwtUtil) {
        this.taskService = taskService;
        this.jwtUtil = jwtUtil;
    }

    /**
     * 从请求头获取用户ID
     */
    private Long getUserIdFromToken(String authHeader) {
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            throw new RuntimeException("未授权");
        }
        String token = authHeader.substring(7);
        return jwtUtil.extractUserId(token);
    }

    /**
     * 创建任务
     */
    @PostMapping
    @Operation(summary = "创建任务", description = "提交视频URL创建处理任务")
    public ResponseEntity<?> createTask(@Valid @RequestBody CreateTaskRequest request,
                                       @RequestHeader("Authorization") String authHeader) {
        try {
            Long userId = getUserIdFromToken(authHeader);
            Task task = taskService.createTask(request.getVideoUrl(), userId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("taskId", task.getTaskId());
            response.put("status", task.getStatus());
            response.put("message", "任务已提交");
            
            return ResponseEntity.ok(response);
        } catch (RuntimeException e) {
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * 获取用户的所有任务
     */
    @GetMapping
    @Operation(summary = "获取任务列表", description = "查询当前用户的所有任务")
    public ResponseEntity<List<Task>> getUserTasks(@RequestHeader("Authorization") String authHeader) {
        Long userId = getUserIdFromToken(authHeader);
        List<Task> tasks = taskService.getUserTasks(userId);
        return ResponseEntity.ok(tasks);
    }

    /**
     * 获取任务详情
     */
    @GetMapping("/{taskId}")
    @Operation(summary = "获取任务详情", description = "查询指定任务的详细信息")
    public ResponseEntity<?> getTaskDetail(@PathVariable String taskId,
                                          @RequestHeader("Authorization") String authHeader) {
        try {
            Long userId = getUserIdFromToken(authHeader);
            Task task = taskService.getTaskDetail(taskId, userId);
            return ResponseEntity.ok(task);
        } catch (RuntimeException e) {
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }

    /**
     * 获取今日剩余次数
     */
    @GetMapping("/quota")
    @Operation(summary = "查询配额", description = "查询今日剩余提交次数")
    public ResponseEntity<?> getQuota(@RequestHeader("Authorization") String authHeader) {
        Long userId = getUserIdFromToken(authHeader);
        int remaining = taskService.getRemainingToday(userId);
        
        Map<String, Object> response = new HashMap<>();
        response.put("dailyLimit", 3);
        response.put("remaining", remaining);
        response.put("used", 3 - remaining);
        
        return ResponseEntity.ok(response);
    }

    /**
     * 下载结果文件
     */
    @GetMapping("/{taskId}/download")
    @Operation(summary = "下载结果", description = "下载任务生成的Markdown文件")
    public ResponseEntity<?> downloadResult(@PathVariable String taskId) {
        try {
            return taskService.downloadResult(taskId);
        } catch (Exception e) {
            Map<String, String> error = new HashMap<>();
            error.put("error", e.getMessage());
            return ResponseEntity.badRequest().body(error);
        }
    }
}
