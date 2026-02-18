package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.common.UserFacingErrorMapper;
import com.mvp.module2.fusion.common.VideoInputNormalizer;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.resilience.ResilientGrpcClient;
import com.mvp.module2.fusion.resilience.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 视频处理 REST API 控制器
 * 
 * 端点：
 * POST /api/tasks          - 提交新任务
 * POST /api/tasks/upload   - 上传视频并提交任务
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
    private static final Pattern UNSAFE_FILENAME_CHARS = Pattern.compile("[^A-Za-z0-9._-]");
    private static final Set<String> ALLOWED_VIDEO_EXTENSIONS = Set.of(".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v");
    private static final long MAX_UPLOAD_FILE_BYTES = 2L * 1024L * 1024L * 1024L;

    @Value("${task.upload.dir:var/uploads}")
    private String uploadDir;
    
    @Autowired
    private TaskQueueManager taskQueueManager;
    
    @Autowired
    private ResilientGrpcClient grpcClient;
    
    /**
     * 提交新任务
     */
    @PostMapping("/tasks")
    public ResponseEntity<Map<String, Object>> submitTask(@RequestBody TaskSubmitRequest request) {
        String normalizedVideoInput = normalizeVideoInput(request.videoUrl);
        if (normalizedVideoInput.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "message", "videoUrl 不能为空"
            ));
        }
        String normalizedUserId = normalizeUserId(request.userId);
        TaskQueueManager.Priority priority = resolvePriority(normalizedUserId, request.priority);
        logger.info("Received task submission: raw={} normalized={} user={}", request.videoUrl, normalizedVideoInput, normalizedUserId);

        // 提交任务
        TaskQueueManager.TaskEntry task = taskQueueManager.submitTask(
            normalizedUserId,
            normalizedVideoInput,
            normalizeOutputDir(request.outputDir),
            priority
        );
        
        return ResponseEntity.ok(Map.of(
            "success", true,
            "taskId", task.taskId,
            "status", task.status.name(),
            "normalizedVideoUrl", normalizedVideoInput,
            "message", "任务已提交，正在排队中"
        ));
    }

    /**
     * 上传视频并提交任务
     */
    @PostMapping(value = "/tasks/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> submitUploadTask(
            @RequestParam("videoFile") MultipartFile videoFile,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam(value = "outputDir", required = false) String outputDir,
            @RequestParam(value = "priority", required = false) String priority
    ) {
        if (videoFile == null || videoFile.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "message", "videoFile 不能为空"
            ));
        }
        if (videoFile.getSize() > MAX_UPLOAD_FILE_BYTES) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "message", "上传文件过大，当前限制 2048MB"
            ));
        }

        String safeFileName = sanitizeUploadFileName(videoFile.getOriginalFilename());
        if (!hasSupportedVideoExtension(safeFileName)) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "message", "仅支持常见视频格式：mp4/mov/mkv/avi/webm/m4v"
            ));
        }

        String normalizedUserId = normalizeUserId(userId);
        TaskQueueManager.Priority taskPriority = resolvePriority(normalizedUserId, priority);

        try {
            Path savedVideoPath = persistUploadedVideo(videoFile, safeFileName);
            logger.info(
                "Received upload task submission: file={} size={} user={} path={}",
                safeFileName,
                videoFile.getSize(),
                normalizedUserId,
                savedVideoPath
            );

            TaskQueueManager.TaskEntry task = taskQueueManager.submitTask(
                normalizedUserId,
                savedVideoPath.toString(),
                normalizeOutputDir(outputDir),
                taskPriority
            );

            return ResponseEntity.ok(Map.of(
                "success", true,
                "taskId", task.taskId,
                "status", task.status.name(),
                "normalizedVideoUrl", savedVideoPath.toString(),
                "uploadedFileName", safeFileName,
                "message", "视频已上传，任务已提交，正在排队中"
            ));
        } catch (IOException e) {
            logger.error("Failed to save uploaded video", e);
            return ResponseEntity.status(503).body(Map.of(
                "success", false,
                "message", UserFacingErrorMapper.busyMessage()
            ));
        }
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
        boolean healthy = grpcClient.isHealthy();
        CircuitBreaker.State cbState = grpcClient.getCircuitBreakerState();
        stats.put("healthy", healthy);
        stats.put("systemStatus", toUserSystemStatus(healthy, cbState));
        return ResponseEntity.ok(stats);
    }

    private String toUserSystemStatus(boolean healthy, CircuitBreaker.State cbState) {
        if (healthy && cbState == CircuitBreaker.State.CLOSED) {
            return "HEALTHY";
        }
        return "BUSY";
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
     * 用户视角健康状态（不暴露内部弹性组件术语）
     */
    @GetMapping("/health/user")
    public ResponseEntity<Map<String, Object>> userHealthCheck() {
        boolean pythonHealthy = grpcClient.isHealthy();
        CircuitBreaker.State cbState = grpcClient.getCircuitBreakerState();
        String systemStatus = toUserSystemStatus(pythonHealthy, cbState);
        boolean healthy = "HEALTHY".equals(systemStatus);

        Map<String, Object> health = Map.of(
            "status", healthy ? "UP" : "BUSY",
            "systemStatus", systemStatus,
            "message", healthy ? "系统运行正常" : UserFacingErrorMapper.busyMessage()
        );

        return healthy
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
        public String priority; // 已废弃：前台不再暴露，服务端将忽略客户端传值
    }

    private String normalizeVideoInput(String rawVideoInput) {
        return VideoInputNormalizer.normalizeVideoInput(rawVideoInput);
    }

    private TaskQueueManager.Priority resolvePriority(String normalizedUserId, String rawPriority) {
        if (StringUtils.hasText(rawPriority)) {
            logger.info("Ignore client priority '{}' for user={}", rawPriority, normalizedUserId);
        }
        return TaskQueueManager.Priority.NORMAL;
    }

    private String normalizeUserId(String rawUserId) {
        String trimmed = rawUserId != null ? rawUserId.trim() : "";
        if (!trimmed.isEmpty()) {
            return trimmed;
        }
        return "user_" + System.currentTimeMillis();
    }

    private String normalizeOutputDir(String rawOutputDir) {
        if (rawOutputDir == null) {
            return null;
        }
        String trimmed = rawOutputDir.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String sanitizeUploadFileName(String rawFileName) {
        String baseName = "uploaded_video.mp4";
        if (StringUtils.hasText(rawFileName)) {
            try {
                baseName = Paths.get(rawFileName).getFileName().toString();
            } catch (Exception ignored) {
                baseName = rawFileName;
            }
        }
        String sanitized = UNSAFE_FILENAME_CHARS.matcher(baseName).replaceAll("_");
        sanitized = sanitized.replaceAll("_+", "_");
        if (!StringUtils.hasText(sanitized)) {
            sanitized = "uploaded_video.mp4";
        }
        if (sanitized.startsWith(".")) {
            sanitized = "video" + sanitized;
        }
        if (!sanitized.contains(".")) {
            sanitized = sanitized + ".mp4";
        }
        return sanitized;
    }

    private boolean hasSupportedVideoExtension(String fileName) {
        String lower = fileName.toLowerCase(Locale.ROOT);
        for (String ext : ALLOWED_VIDEO_EXTENSIONS) {
            if (lower.endsWith(ext)) {
                return true;
            }
        }
        return false;
    }

    private Path persistUploadedVideo(MultipartFile videoFile, String safeFileName) throws IOException {
        Path uploadRootPath = resolveUploadRoot();
        String uniquePrefix = Instant.now().toEpochMilli() + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        Path targetPath = uploadRootPath.resolve(uniquePrefix + "_" + safeFileName).toAbsolutePath().normalize();
        if (!targetPath.startsWith(uploadRootPath)) {
            throw new IOException("非法上传路径");
        }
        try (InputStream inputStream = videoFile.getInputStream()) {
            Files.copy(inputStream, targetPath, StandardCopyOption.REPLACE_EXISTING);
        }
        return targetPath;
    }

    private Path resolveUploadRoot() throws IOException {
        Path uploadRootPath = Paths.get(uploadDir).toAbsolutePath().normalize();
        Files.createDirectories(uploadRootPath);
        return uploadRootPath;
    }
}
