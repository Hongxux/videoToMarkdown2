package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.common.UserFacingErrorMapper;
import com.mvp.module2.fusion.common.VideoInputNormalizer;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.resilience.ResilientGrpcClient;
import com.mvp.module2.fusion.resilience.CircuitBreaker;
import com.mvp.module2.fusion.service.BookMarkdownService;
import com.mvp.module2.fusion.service.CollectionRepository;
import com.mvp.module2.fusion.service.FileTransferService;
import com.mvp.module2.fusion.service.FileReuseService;
import com.mvp.module2.fusion.service.Phase2bArticleLinkService;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    private static final Set<String> ALLOWED_UPLOAD_EXTENSIONS = Set.of(
        ".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v",
        ".txt", ".md", ".pdf", ".epub"
    );
    private static final long MAX_UPLOAD_FILE_BYTES = 2L * 1024L * 1024L * 1024L;

    @Value("${task.upload.dir:var/uploads}")
    private String uploadDir;

    @Value("${grpc.python.timeout-seconds:300}")
    private int grpcTimeoutSeconds;
    
    @Autowired
    private TaskQueueManager taskQueueManager;
    
    @Autowired
    private ResilientGrpcClient grpcClient;

    @Autowired
    private PythonGrpcClient pythonGrpcClient;

    @Autowired(required = false)
    private CollectionRepository collectionRepository;

    @Autowired(required = false)
    private BookMarkdownService bookMarkdownService;

    @Autowired(required = false)
    private FileReuseService fileReuseService;

    @Autowired(required = false)
    private Phase2bArticleLinkService phase2bArticleLinkService;

    @Autowired
    private FileTransferService fileTransferService;
    
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
        TaskQueueManager.BookProcessingOptions bookOptions = buildBookProcessingOptions(
            request.chapterSelector,
            request.sectionSelector,
            request.splitByChapter,
            request.splitBySection,
            request.pageOffset
        );
        logger.info("Received task submission: raw={} normalized={} user={}", request.videoUrl, normalizedVideoInput, normalizedUserId);

        // 提交任务
        TaskQueueManager.TaskEntry task = taskQueueManager.submitTask(
            normalizedUserId,
            normalizedVideoInput,
            normalizeOutputDir(request.outputDir),
            priority,
            null,
            bookOptions
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
            @RequestParam(value = "priority", required = false) String priority,
            @RequestParam(value = "chapterSelector", required = false) String chapterSelector,
            @RequestParam(value = "sectionSelector", required = false) String sectionSelector,
            @RequestParam(value = "splitByChapter", required = false) Boolean splitByChapter,
            @RequestParam(value = "splitBySection", required = false) Boolean splitBySection,
            @RequestParam(value = "pageOffset", required = false) Integer pageOffset,
            @RequestParam(value = "probeOnly", required = false) Boolean probeOnly,
            @RequestParam(value = "fileMd5", required = false) String fileMd5,
            @RequestParam(value = "fileExt", required = false) String fileExt
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
        if (!hasSupportedUploadExtension(safeFileName)) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "message", "仅支持：mp4/mov/mkv/avi/webm/m4v/txt/md/pdf/epub"
            ));
        }

        String normalizedUserId = normalizeUserId(userId);
        TaskQueueManager.Priority taskPriority = resolvePriority(normalizedUserId, priority);
        Optional<FileReuseService.FileFingerprint> fingerprintOpt =
                resolveFileFingerprint(fileMd5, fileExt, safeFileName);
        Optional<Path> reusedPathOpt = findReusableUploadPath(fingerprintOpt);
        if (reusedPathOpt.isPresent()) {
            Path reusedPath = reusedPathOpt.get();
            if (Boolean.TRUE.equals(probeOnly)) {
                Map<String, Object> payload = new LinkedHashMap<>();
                payload.put("success", true);
                payload.put("probeOnly", true);
                payload.put("reused", true);
                payload.put("normalizedVideoUrl", reusedPath.toString());
                payload.put("uploadedFileName", safeFileName);
                appendFingerprintPayload(payload, fingerprintOpt);
                appendProbeCachePayload(payload, fingerprintOpt);
                payload.put("message", "file reused for probe");
                return ResponseEntity.ok(payload);
            }
            TaskQueueManager.BookProcessingOptions bookOptions = buildBookProcessingOptions(
                    chapterSelector,
                    sectionSelector,
                    splitByChapter,
                    splitBySection,
                    pageOffset
            );
            TaskQueueManager.TaskEntry task = taskQueueManager.submitTask(
                    normalizedUserId,
                    reusedPath.toString(),
                    normalizeOutputDir(outputDir),
                    taskPriority,
                    null,
                    bookOptions
            );
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("reused", true);
            payload.put("taskId", task.taskId);
            payload.put("status", task.status.name());
            payload.put("normalizedVideoUrl", reusedPath.toString());
            payload.put("uploadedFileName", safeFileName);
            appendFingerprintPayload(payload, fingerprintOpt);
            payload.put("message", "file reused; task submitted and queued");
            return ResponseEntity.ok(payload);
        }

        try {
            Path savedVideoPath = persistUploadedVideo(videoFile, safeFileName);
            recordUploadedFileMetadata(savedVideoPath, safeFileName, videoFile.getSize(), fingerprintOpt);
            logger.info(
                "Received upload task submission: file={} size={} user={} path={}",
                safeFileName,
                videoFile.getSize(),
                normalizedUserId,
                savedVideoPath
            );

            if (Boolean.TRUE.equals(probeOnly)) {
                return ResponseEntity.ok(Map.of(
                    "success", true,
                    "probeOnly", true,
                    "normalizedVideoUrl", savedVideoPath.toString(),
                    "uploadedFileName", safeFileName,
                    "message", "文件已上传，可继续探测章节"
                ));
            }

            TaskQueueManager.BookProcessingOptions bookOptions = buildBookProcessingOptions(
                chapterSelector,
                sectionSelector,
                splitByChapter,
                splitBySection,
                pageOffset
            );
            TaskQueueManager.TaskEntry task = taskQueueManager.submitTask(
                normalizedUserId,
                savedVideoPath.toString(),
                normalizeOutputDir(outputDir),
                taskPriority,
                null,
                bookOptions
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

    @GetMapping({"/video-info", "/mobile/video-info"})
    public ResponseEntity<Map<String, Object>> getVideoInfo(
            @RequestParam("videoInput") String videoInput,
            @RequestParam(value = "pageOffset", required = false) Integer pageOffset,
            @RequestParam(value = "fileMd5", required = false) String fileMd5,
            @RequestParam(value = "fileExt", required = false) String fileExt
    ) {
        return queryVideoInfoInternal(videoInput, pageOffset, fileMd5, fileExt);
    }

    @PostMapping({"/video-info", "/mobile/video-info"})
    public ResponseEntity<Map<String, Object>> getVideoInfoByPost(@RequestBody VideoInfoRequest request) {
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "message", "request body cannot be empty"
            ));
        }
        return queryVideoInfoInternal(request.videoInput, request.pageOffset, request.fileMd5, request.fileExt);
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
        public String priority;
        public String chapterSelector;
        public String sectionSelector;
        public Boolean splitByChapter;
        public Boolean splitBySection;
        public Integer pageOffset;
    }

    public static class VideoInfoRequest {
        public String videoInput;
        public Integer pageOffset;
        public String fileMd5;
        public String fileExt;
    }

    private String normalizeVideoInput(String rawVideoInput) {
        return VideoInputNormalizer.normalizeVideoInput(rawVideoInput);
    }

    private ResponseEntity<Map<String, Object>> queryVideoInfoInternal(
            String rawVideoInput,
            Integer pageOffset,
            String rawFileMd5,
            String rawFileExt
    ) {
        String rawInput = rawVideoInput != null ? rawVideoInput.trim() : "";
        if (rawInput.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "message", "videoInput 不能为空"
            ));
        }

        String normalizedInput = normalizeVideoInput(rawInput);
        String probeInput = chooseVideoInfoProbeInput(rawInput, normalizedInput);
        Optional<FileReuseService.FileFingerprint> probeFingerprintOpt =
                resolveProbeFingerprint(rawFileMd5, rawFileExt, probeInput);
        Optional<Map<String, Object>> cachedProbePayloadOpt = findCachedProbePayload(
                probeFingerprintOpt,
                rawInput,
                normalizedInput,
                probeInput
        );
        if (cachedProbePayloadOpt.isPresent()) {
            return ResponseEntity.ok(cachedProbePayloadOpt.get());
        }
        if (isBookProbeInput(probeInput)) {
            ResponseEntity<Map<String, Object>> response =
                    probeBookInfo(rawInput, normalizedInput, probeInput, pageOffset);
            return enrichAndPersistProbePayload(response, probeFingerprintOpt);
        }
        if (isArticleProbeInput(probeInput)) {
            ResponseEntity<Map<String, Object>> response =
                    probeArticleInfo(rawInput, normalizedInput, probeInput);
            return enrichAndPersistProbePayload(response, probeFingerprintOpt);
        }
        String taskId = "VI_" + System.currentTimeMillis();

        PythonGrpcClient.VideoInfoResult result = pythonGrpcClient.getVideoInfo(
            taskId,
            probeInput,
            Math.max(30, grpcTimeoutSeconds)
        );

        if (result == null || !result.success) {
            String detail = result != null ? result.errorMsg : "empty grpc response";
            logger.warn("GetVideoInfo failed: raw={} normalized={} probe={} err={}", rawInput, normalizedInput, probeInput, detail);
            return ResponseEntity.status(502).body(Map.of(
                "success", false,
                "message", UserFacingErrorMapper.busyMessage(),
                "detail", detail != null ? detail : ""
            ));
        }

        List<Map<String, Object>> episodes = new ArrayList<>();
        List<String> episodeTitles = new ArrayList<>();
        if (result.episodes != null) {
            for (PythonGrpcClient.EpisodeInfo episode : result.episodes) {
                if (episode == null) {
                    continue;
                }
                String title = episode.title != null ? episode.title : "";
                episodes.add(Map.of(
                    "index", episode.index,
                    "title", title,
                    "durationSec", episode.durationSec,
                    "episodeUrl", episode.episodeUrl != null ? episode.episodeUrl : "",
                    "episodeCoverUrl", episode.episodeCoverUrl != null ? episode.episodeCoverUrl : ""
                ));
                episodeTitles.add(title);
            }
        }
        int normalizedTotalEpisodes = episodes.isEmpty()
                ? Math.max(0, result.totalEpisodes)
                : episodes.size();
        String collectionId = buildCollectionId(result.sourcePlatform, result.canonicalId);
        if (result.isCollection && !collectionId.isEmpty()) {
            persistCollectionInfo(collectionId, result);
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("rawInput", rawInput);
        payload.put("normalizedVideoInput", normalizedInput);
        payload.put("probeInput", probeInput);
        payload.put("platform", result.sourcePlatform != null ? result.sourcePlatform : "");
        payload.put("resolvedUrl", result.resolvedUrl != null ? result.resolvedUrl : "");
        payload.put("canonicalId", result.canonicalId != null ? result.canonicalId : "");
        payload.put("collectionId", collectionId);
        payload.put("rawEncodingKey", buildRawEncodingKey(result));
        payload.put("title", result.videoTitle != null ? result.videoTitle : "");
        payload.put("durationSec", result.durationSec);
        payload.put("isCollection", result.isCollection);
        payload.put("totalEpisodes", normalizedTotalEpisodes);
        payload.put("currentEpisodeIndex", result.currentEpisodeIndex);
        payload.put("currentEpisodeTitle", result.currentEpisodeTitle != null ? result.currentEpisodeTitle : "");
        payload.put("episodes", episodes);
        payload.put("episodeTitles", episodeTitles);
        payload.put("coverUrl", result.coverUrl != null ? result.coverUrl : "");
        payload.put("contentType", result.contentType != null ? result.contentType : "");
        payload.put("linkResolver", result.linkResolver != null ? result.linkResolver : "");
        return enrichAndPersistProbePayload(ResponseEntity.ok(payload), probeFingerprintOpt);
    }

    private Optional<FileReuseService.FileFingerprint> resolveFileFingerprint(
            String rawMd5,
            String rawExt,
            String fallbackFileName
    ) {
        if (fileReuseService == null) {
            return Optional.empty();
        }
        return fileReuseService.normalizeFingerprint(rawMd5, rawExt, fallbackFileName);
    }

    private Optional<Path> findReusableUploadPath(Optional<FileReuseService.FileFingerprint> fingerprintOpt) {
        if (fileReuseService == null || fingerprintOpt == null || fingerprintOpt.isEmpty()) {
            return Optional.empty();
        }
        return fileReuseService.findReusablePath(fingerprintOpt.get());
    }

    private void recordUploadedFileMetadata(
            Path filePath,
            String safeFileName,
            long fileSize,
            Optional<FileReuseService.FileFingerprint> fingerprintOpt
    ) {
        if (fileReuseService == null || filePath == null) {
            return;
        }
        Long normalizedSize = fileSize >= 0 ? fileSize : null;
        if (fingerprintOpt != null && fingerprintOpt.isPresent()) {
            fileReuseService.recordUploadedFile(fingerprintOpt.get(), filePath, normalizedSize, safeFileName);
            return;
        }
        fileReuseService.recordUploadedFileAsync(filePath, safeFileName, normalizedSize);
    }

    private void appendFingerprintPayload(
            Map<String, Object> payload,
            Optional<FileReuseService.FileFingerprint> fingerprintOpt
    ) {
        if (payload == null || fingerprintOpt == null || fingerprintOpt.isEmpty()) {
            return;
        }
        payload.put("fileMd5", fingerprintOpt.get().md5());
        payload.put("fileExt", fingerprintOpt.get().fileExt());
    }

    private void appendProbeCachePayload(
            Map<String, Object> payload,
            Optional<FileReuseService.FileFingerprint> fingerprintOpt
    ) {
        if (payload == null || fileReuseService == null || fingerprintOpt == null || fingerprintOpt.isEmpty()) {
            return;
        }
        Optional<Map<String, Object>> probePayloadOpt = fileReuseService.findProbePayload(fingerprintOpt.get());
        if (probePayloadOpt.isEmpty()) {
            payload.put("probeCacheHit", false);
            return;
        }
        payload.put("probeCacheHit", true);
        payload.put("probePayload", probePayloadOpt.get());
    }

    private Optional<FileReuseService.FileFingerprint> resolveProbeFingerprint(
            String rawFileMd5,
            String rawFileExt,
            String probeInput
    ) {
        Optional<FileReuseService.FileFingerprint> hintFingerprintOpt =
                resolveFileFingerprint(rawFileMd5, rawFileExt, probeInput);
        if (hintFingerprintOpt.isPresent()) {
            return hintFingerprintOpt;
        }
        if (fileReuseService == null || !isLocalFilePath(probeInput)) {
            return Optional.empty();
        }
        return fileReuseService.findFingerprintByPath(probeInput);
    }

    private Optional<Map<String, Object>> findCachedProbePayload(
            Optional<FileReuseService.FileFingerprint> fingerprintOpt,
            String rawInput,
            String normalizedInput,
            String probeInput
    ) {
        if (fileReuseService == null || fingerprintOpt == null || fingerprintOpt.isEmpty()) {
            return Optional.empty();
        }
        Optional<Map<String, Object>> payloadOpt = fileReuseService.findProbePayload(fingerprintOpt.get());
        if (payloadOpt.isEmpty()) {
            return Optional.empty();
        }
        Map<String, Object> payload = new LinkedHashMap<>(payloadOpt.get());
        payload.put("success", true);
        payload.put("rawInput", rawInput);
        payload.put("normalizedVideoInput", normalizedInput);
        payload.put("probeInput", probeInput);
        payload.put("probeCacheHit", true);
        appendFingerprintPayload(payload, fingerprintOpt);
        return Optional.of(payload);
    }

    private ResponseEntity<Map<String, Object>> enrichAndPersistProbePayload(
            ResponseEntity<Map<String, Object>> response,
            Optional<FileReuseService.FileFingerprint> fingerprintOpt
    ) {
        if (response == null || response.getBody() == null || fingerprintOpt == null || fingerprintOpt.isEmpty()) {
            return response;
        }
        Map<String, Object> payload = new LinkedHashMap<>(response.getBody());
        appendFingerprintPayload(payload, fingerprintOpt);
        payload.put("probeCacheHit", false);
        if (response.getStatusCode().is2xxSuccessful() && Boolean.TRUE.equals(payload.get("success")) && fileReuseService != null) {
            Map<String, Object> cachePayload = new LinkedHashMap<>(payload);
            cachePayload.remove("probeCacheHit");
            fileReuseService.recordProbePayload(fingerprintOpt.get(), cachePayload);
        }
        return ResponseEntity.status(response.getStatusCode()).body(payload);
    }

    private boolean isLocalFilePath(String probeInput) {
        if (probeInput == null || probeInput.isBlank()) {
            return false;
        }
        String normalized = probeInput.trim().toLowerCase(Locale.ROOT);
        if (normalized.startsWith("http://") || normalized.startsWith("https://")) {
            return false;
        }
        try {
            Path path = Paths.get(probeInput).toAbsolutePath().normalize();
            return Files.exists(path);
        } catch (Exception ex) {
            return false;
        }
    }

    private void persistCollectionInfo(String collectionId, PythonGrpcClient.VideoInfoResult result) {
        if (collectionRepository == null || result == null) {
            return;
        }
        try {
            List<CollectionRepository.EpisodeInput> episodeInputs = new ArrayList<>();
            if (result.episodes != null) {
                for (PythonGrpcClient.EpisodeInfo episode : result.episodes) {
                    if (episode == null) {
                        continue;
                    }
                    int episodeNo = episode.index > 0 ? episode.index : episodeInputs.size() + 1;
                    episodeInputs.add(new CollectionRepository.EpisodeInput(
                            episodeNo,
                            episode.title,
                            episode.episodeUrl,
                            episode.durationSec
                    ));
                }
            }
            int totalEpisodes = result.totalEpisodes > 0 ? result.totalEpisodes : episodeInputs.size();
            collectionRepository.upsertCollection(
                    collectionId,
                    result.sourcePlatform,
                    result.canonicalId,
                    result.videoTitle,
                    totalEpisodes,
                    result.resolvedUrl,
                    episodeInputs
            );
        } catch (Exception ex) {
            logger.warn("persist collection info failed: collectionId={} err={}", collectionId, ex.getMessage());
        }
    }

    private String buildCollectionId(String platform, String canonicalId) {
        String normalizedPlatform = platform != null ? platform.trim() : "";
        String normalizedCanonicalId = canonicalId != null ? canonicalId.trim() : "";
        if (normalizedPlatform.isEmpty() || normalizedCanonicalId.isEmpty()) {
            return "";
        }
        return normalizedPlatform.toLowerCase(Locale.ROOT) + ":" + normalizedCanonicalId;
    }

    private String buildRawEncodingKey(PythonGrpcClient.VideoInfoResult result) {
        if (result == null) {
            return "";
        }
        String canonicalId = result.canonicalId != null ? result.canonicalId.trim() : "";
        if (canonicalId.isEmpty()) {
            return "";
        }
        String platform = result.sourcePlatform != null ? result.sourcePlatform.trim() : "";
        if (!"bilibili".equalsIgnoreCase(platform)) {
            return canonicalId;
        }
        if (result.currentEpisodeIndex > 0) {
            return canonicalId + "_" + result.currentEpisodeIndex;
        }
        return canonicalId;
    }

    private ResponseEntity<Map<String, Object>> probeBookInfo(
            String rawInput,
            String normalizedInput,
            String probeInput,
            Integer pageOffset
    ) {
        if (bookMarkdownService == null) {
            return ResponseEntity.status(503).body(Map.of(
                    "success", false,
                    "message", "book probe service unavailable"
            ));
        }
        BookMarkdownService.BookProbeResult probe = bookMarkdownService.probeBook(probeInput, pageOffset);
        if (probe == null || !probe.success) {
            String detail = probe != null ? probe.errorMessage : "empty book probe response";
            logger.warn("book probe failed: raw={} normalized={} probe={} err={}", rawInput, normalizedInput, probeInput, detail);
            return ResponseEntity.status(502).body(Map.of(
                    "success", false,
                    "message", UserFacingErrorMapper.busyMessage(),
                    "detail", detail != null ? detail : ""
            ));
        }

        List<Map<String, Object>> episodes = new ArrayList<>();
        List<String> episodeTitles = new ArrayList<>();
        if (false && probe.sections != null) {
            for (Map<String, Object> section : probe.sections) {
                if (section == null) {
                    continue;
                }
                int flatIndex = asInt(section.get("flatIndex"), episodes.size() + 1);
                String sectionTitle = asText(section.get("title"));
                String chapterTitle = asText(section.get("chapterTitle"));
                int chapterIndex = asInt(section.get("chapterIndex"), 0);
                int sectionIndex = asInt(section.get("sectionIndex"), 0);
                int startPage = asInt(section.get("startPage"), -1);
                int endPage = asInt(section.get("endPage"), -1);
                String baseSectionSelector = asText(section.get("sectionSelector"));

                int normalizedStartPage = startPage > 0 ? startPage : -1;
                int normalizedEndPage = endPage >= normalizedStartPage && normalizedStartPage > 0
                        ? endPage
                        : normalizedStartPage;
                int leafCount = (normalizedStartPage > 0 && normalizedEndPage >= normalizedStartPage)
                        ? Math.max(1, normalizedEndPage - normalizedStartPage + 1)
                        : 1;

                for (int leafIndex = 1; leafIndex <= leafCount; leafIndex++) {
                    int leafFlatIndex = episodes.size() + 1;
                    int leafStartPage = normalizedStartPage > 0 ? (normalizedStartPage + leafIndex - 1) : startPage;
                    int leafEndPage = normalizedStartPage > 0 ? leafStartPage : endPage;
                    String outlineIndex = buildBookOutlineIndex(chapterIndex, sectionIndex, leafIndex, leafFlatIndex);
                    String leafTitle = leafCount > 1 && leafStartPage > 0
                            ? firstNonBlank(sectionTitle, "未命名章节") + " · 第" + leafStartPage + "页"
                            : sectionTitle;

                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("index", leafFlatIndex);
                    item.put("flatIndex", leafFlatIndex);
                    item.put("title", leafTitle);
                    item.put("chapterTitle", chapterTitle);
                    item.put("chapterIndex", chapterIndex);
                    item.put("sectionIndex", sectionIndex);
                    item.put("subSectionIndex", leafIndex);
                    item.put("outlineIndex", outlineIndex);
                    item.put("startPage", leafStartPage);
                    item.put("endPage", leafEndPage);
                    item.put("baseSectionSelector", baseSectionSelector);
                    item.put("sectionSelector", buildLeafSectionSelector(baseSectionSelector, chapterIndex, sectionIndex, leafIndex));
                    item.put("episodeUrl", probeInput);
                    item.put("durationSec", null);
                    episodes.add(item);
                    episodeTitles.add(leafTitle);
                }
            }
        }
        episodes = buildBookProbeEpisodes(probe, probeInput);
        episodeTitles = collectEpisodeTitles(episodes);

        String canonicalId;
        try {
            canonicalId = Paths.get(probeInput).getFileName() != null
                    ? Paths.get(probeInput).getFileName().toString()
                    : probeInput;
        } catch (Exception ignored) {
            canonicalId = probeInput;
        }

        int detectedStartPage = resolveBookStartPage(probe.detectedPageOffset, probe.sections, probe.totalPages);
        int appliedStartPage = resolveBookStartPage(probe.appliedPageOffset, probe.sections, probe.totalPages);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("rawInput", rawInput);
        payload.put("normalizedVideoInput", normalizedInput);
        payload.put("probeInput", probeInput);
        payload.put("platform", "book");
        payload.put("resolvedUrl", probeInput);
        payload.put("canonicalId", canonicalId);
        payload.put("collectionId", "");
        payload.put("rawEncodingKey", canonicalId);
        payload.put("title", probe.bookTitle != null ? probe.bookTitle : "");
        payload.put("durationSec", null);
        payload.put("isCollection", true);
        payload.put("totalEpisodes", episodes.size());
        payload.put("currentEpisodeIndex", 0);
        payload.put("currentEpisodeTitle", "");
        payload.put("episodes", episodes);
        payload.put("episodeTitles", episodeTitles);
        payload.put("coverUrl", "");
        payload.put("contentType", "book");
        payload.put("linkResolver", "book_probe");
        payload.put("totalPages", probe.totalPages);
        payload.put("bookChapters", probe.chapters != null ? probe.chapters : List.of());
        payload.put("bookSections", probe.sections != null ? probe.sections : List.of());
        payload.put("bookLeafSections", probe.leafSections != null ? probe.leafSections : List.of());
        payload.put("bookSectionTree", buildBookSectionTree(episodes));
        payload.put("bookChapterCount", probe.chapterCount);
        payload.put("bookSectionCount", probe.sectionCount);
        payload.put("bookLeafSectionCount", probe.leafSections != null ? probe.leafSections.size() : 0);
        payload.put("detectedPageOffset", probe.detectedPageOffset);
        payload.put("appliedPageOffset", probe.appliedPageOffset);
        payload.put("detectedStartPage", detectedStartPage);
        payload.put("confirmedStartPage", appliedStartPage);
        payload.put("pageMapStrategy", probe.pageMapStrategy != null ? probe.pageMapStrategy : "");
        return ResponseEntity.ok(payload);
    }

    private boolean isBookProbeInput(String probeInput) {
        if (probeInput == null || probeInput.isBlank()) {
            return false;
        }
        return isBookPathLike(probeInput);
    }

    private boolean isArticleProbeInput(String probeInput) {
        if (probeInput == null || probeInput.isBlank() || phase2bArticleLinkService == null) {
            return false;
        }
        try {
            List<String> normalized = phase2bArticleLinkService.normalizeSupportedLinks(List.of(probeInput));
            return normalized != null && !normalized.isEmpty();
        } catch (Exception error) {
            logger.warn("probe article input normalize failed: input={} err={}", probeInput, error.getMessage());
            return false;
        }
    }

    private ResponseEntity<Map<String, Object>> probeArticleInfo(
            String rawInput,
            String normalizedInput,
            String probeInput
    ) {
        if (phase2bArticleLinkService == null) {
            return ResponseEntity.status(503).body(Map.of(
                    "success", false,
                    "message", "article link probe service unavailable"
            ));
        }
        List<String> normalizedLinks = phase2bArticleLinkService.normalizeSupportedLinks(List.of(probeInput));
        if (normalizedLinks == null || normalizedLinks.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "unsupported article link"
            ));
        }
        String resolvedUrl = normalizedLinks.get(0);
        String title = "";
        String siteType = inferArticleSiteType(resolvedUrl);
        try {
            List<Phase2bArticleLinkService.LinkMetadata> metadataList =
                    phase2bArticleLinkService.prefetchLinkMetadata(List.of(resolvedUrl));
            if (metadataList != null && !metadataList.isEmpty()) {
                Phase2bArticleLinkService.LinkMetadata metadata = metadataList.get(0);
                if (metadata != null) {
                    title = firstNonBlank(metadata.title, title);
                    siteType = firstNonBlank(metadata.siteType, siteType);
                }
            }
        } catch (Exception error) {
            logger.warn("probe article metadata prefetch failed: url={} err={}", resolvedUrl, error.getMessage());
        }
        if (!StringUtils.hasText(title)) {
            title = resolvedUrl;
        }
        Map<String, Object> episode = new LinkedHashMap<>();
        episode.put("index", 1);
        episode.put("title", title);
        episode.put("durationSec", null);
        episode.put("episodeUrl", resolvedUrl);
        episode.put("episodeCoverUrl", "");

        List<Map<String, Object>> episodes = List.of(episode);
        List<String> episodeTitles = List.of(title);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("rawInput", rawInput);
        payload.put("normalizedVideoInput", normalizedInput);
        payload.put("probeInput", resolvedUrl);
        payload.put("platform", siteType);
        payload.put("resolvedUrl", resolvedUrl);
        payload.put("canonicalId", resolvedUrl);
        payload.put("collectionId", "");
        payload.put("rawEncodingKey", resolvedUrl);
        payload.put("title", title);
        payload.put("durationSec", null);
        payload.put("isCollection", false);
        payload.put("totalEpisodes", 1);
        payload.put("currentEpisodeIndex", 1);
        payload.put("currentEpisodeTitle", title);
        payload.put("episodes", episodes);
        payload.put("episodeTitles", episodeTitles);
        payload.put("coverUrl", "");
        payload.put("contentType", "book");
        payload.put("linkResolver", "phase2b_article_link_probe");
        return ResponseEntity.ok(payload);
    }

    private String inferArticleSiteType(String url) {
        String normalized = String.valueOf(url == null ? "" : url).toLowerCase(Locale.ROOT);
        if (normalized.contains("zhihu.com")) {
            return "zhihu";
        }
        if (normalized.contains("juejin.cn")) {
            return "juejin";
        }
        return "article";
    }

    private boolean isBookPathLike(String pathLike) {
        String normalized = pathLike != null ? pathLike.trim().toLowerCase(Locale.ROOT) : "";
        if (normalized.isBlank()) {
            return false;
        }
        int queryAt = normalized.indexOf('?');
        if (queryAt > 0) {
            normalized = normalized.substring(0, queryAt);
        }
        int fragmentAt = normalized.indexOf('#');
        if (fragmentAt > 0) {
            normalized = normalized.substring(0, fragmentAt);
        }
        return normalized.endsWith(".pdf")
                || normalized.endsWith(".txt")
                || normalized.endsWith(".md")
                || normalized.endsWith(".epub");
    }

    private int asInt(Object value, int fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private String asText(Object value) {
        if (value == null) {
            return "";
        }
        return String.valueOf(value).trim();
    }

    private String firstNonBlank(String value, String fallback) {
        if (value != null && !value.trim().isBlank()) {
            return value.trim();
        }
        return fallback != null ? fallback : "";
    }

    private String buildBookOutlineIndex(int chapterIndex, int sectionIndex, int subSectionIndex, int fallbackIndex) {
        if (chapterIndex > 0 && sectionIndex > 0 && subSectionIndex > 0) {
            return chapterIndex + "." + sectionIndex + "." + subSectionIndex;
        }
        return "0.0." + Math.max(1, fallbackIndex);
    }

    private String buildLeafSectionSelector(String baseSelector, int chapterIndex, int sectionIndex, int subSectionIndex) {
        String normalizedBase = asText(baseSelector);
        if (normalizedBase.isBlank() && chapterIndex > 0 && sectionIndex > 0) {
            normalizedBase = "c" + chapterIndex + "s" + sectionIndex;
        }
        if (normalizedBase.isBlank()) {
            normalizedBase = "c1s1";
        }
        return normalizedBase + "t" + Math.max(1, subSectionIndex);
    }

    private List<Map<String, Object>> buildBookProbeEpisodes(BookMarkdownService.BookProbeResult probe, String probeInput) {
        List<Map<String, Object>> episodes = new ArrayList<>();
        if (probe == null) {
            return episodes;
        }
        if (probe.leafSections != null && !probe.leafSections.isEmpty()) {
            for (Map<String, Object> leaf : probe.leafSections) {
                if (leaf == null) {
                    continue;
                }
                int leafFlatIndex = episodes.size() + 1;
                int chapterIndex = asInt(leaf.get("chapterIndex"), 0);
                int sectionIndex = asInt(leaf.get("sectionIndex"), 0);
                int subSectionIndex = Math.max(1, asInt(leaf.get("subSectionIndex"), 1));
                int startPage = asInt(leaf.get("startPage"), -1);
                int endPage = asInt(leaf.get("endPage"), startPage);
                if (endPage < startPage) {
                    endPage = startPage;
                }
                String chapterTitle = asText(leaf.get("chapterTitle"));
                String leafTitle = firstNonBlank(asText(leaf.get("title")), "Section " + leafFlatIndex);
                String outlineIndex = firstNonBlank(
                        asText(leaf.get("outlineIndex")),
                        buildBookOutlineIndex(chapterIndex, sectionIndex, subSectionIndex, leafFlatIndex)
                );
                String baseSectionSelector = asText(leaf.get("baseSectionSelector"));
                if (baseSectionSelector.isBlank() && chapterIndex > 0 && sectionIndex > 0) {
                    baseSectionSelector = "c" + chapterIndex + "s" + sectionIndex;
                }
                String sectionSelector = asText(leaf.get("sectionSelector"));
                if (sectionSelector.isBlank()) {
                    sectionSelector = buildLeafSectionSelector(baseSectionSelector, chapterIndex, sectionIndex, subSectionIndex);
                }

                Map<String, Object> item = new LinkedHashMap<>();
                item.put("index", leafFlatIndex);
                item.put("flatIndex", leafFlatIndex);
                item.put("title", leafTitle);
                item.put("chapterTitle", chapterTitle);
                item.put("chapterIndex", chapterIndex);
                item.put("sectionIndex", sectionIndex);
                item.put("subSectionIndex", subSectionIndex);
                item.put("outlineIndex", outlineIndex);
                item.put("startPage", startPage);
                item.put("endPage", endPage);
                item.put("baseSectionSelector", baseSectionSelector);
                item.put("sectionSelector", sectionSelector);
                item.put("episodeUrl", probeInput);
                item.put("durationSec", null);
                episodes.add(item);
            }
            return episodes;
        }
        if (probe.sections == null) {
            return episodes;
        }
        for (Map<String, Object> section : probe.sections) {
            if (section == null) {
                continue;
            }
            int flatIndex = episodes.size() + 1;
            int chapterIndex = asInt(section.get("chapterIndex"), 0);
            int sectionIndex = asInt(section.get("sectionIndex"), 0);
            int subSectionIndex = 1;
            int startPage = asInt(section.get("startPage"), -1);
            int endPage = asInt(section.get("endPage"), startPage);
            if (endPage < startPage) {
                endPage = startPage;
            }
            String chapterTitle = asText(section.get("chapterTitle"));
            String sectionTitle = firstNonBlank(asText(section.get("title")), "Section " + flatIndex);
            String baseSectionSelector = asText(section.get("sectionSelector"));
            if (baseSectionSelector.isBlank() && chapterIndex > 0 && sectionIndex > 0) {
                baseSectionSelector = "c" + chapterIndex + "s" + sectionIndex;
            }

            Map<String, Object> item = new LinkedHashMap<>();
            item.put("index", flatIndex);
            item.put("flatIndex", flatIndex);
            item.put("title", sectionTitle);
            item.put("chapterTitle", chapterTitle);
            item.put("chapterIndex", chapterIndex);
            item.put("sectionIndex", sectionIndex);
            item.put("subSectionIndex", subSectionIndex);
            item.put("outlineIndex", buildBookOutlineIndex(chapterIndex, sectionIndex, subSectionIndex, flatIndex));
            item.put("startPage", startPage);
            item.put("endPage", endPage);
            item.put("baseSectionSelector", baseSectionSelector);
            item.put("sectionSelector", baseSectionSelector);
            item.put("episodeUrl", probeInput);
            item.put("durationSec", null);
            episodes.add(item);
        }
        return episodes;
    }

    private List<String> collectEpisodeTitles(List<Map<String, Object>> episodes) {
        List<String> titles = new ArrayList<>();
        if (episodes == null) {
            return titles;
        }
        for (Map<String, Object> episode : episodes) {
            if (episode == null) {
                continue;
            }
            titles.add(asText(episode.get("title")));
        }
        return titles;
    }

    private List<Map<String, Object>> buildBookSectionTree(List<Map<String, Object>> episodes) {
        Map<String, Map<String, Object>> chapterMap = new LinkedHashMap<>();
        Map<String, Map<String, Object>> sectionMap = new LinkedHashMap<>();
        List<Map<String, Object>> tree = new ArrayList<>();
        if (episodes == null) {
            return tree;
        }
        for (Map<String, Object> episode : episodes) {
            if (episode == null) {
                continue;
            }
            int chapterIndex = asInt(episode.get("chapterIndex"), 0);
            int sectionIndex = asInt(episode.get("sectionIndex"), 0);
            int subSectionIndex = asInt(episode.get("subSectionIndex"), 0);
            int episodeNo = asInt(episode.get("index"), -1);
            String chapterTitle = firstNonBlank(asText(episode.get("chapterTitle")), "第 " + Math.max(1, chapterIndex) + " 章");
            String sectionTitle = firstNonBlank(asText(episode.get("title")), "未命名章节");
            String chapterKey = chapterIndex > 0 ? ("c" + chapterIndex) : ("c_fallback_" + episodeNo);
            String sectionKey = chapterKey + "_s" + Math.max(1, sectionIndex);

            Map<String, Object> chapterNode = chapterMap.get(chapterKey);
            if (chapterNode == null) {
                chapterNode = new LinkedHashMap<>();
                chapterNode.put("nodeType", "chapter");
                chapterNode.put("chapterIndex", chapterIndex);
                chapterNode.put("title", chapterTitle);
                chapterNode.put("children", new ArrayList<Map<String, Object>>());
                chapterMap.put(chapterKey, chapterNode);
                tree.add(chapterNode);
            }

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> chapterChildren = (List<Map<String, Object>>) chapterNode.get("children");
            Map<String, Object> sectionNode = sectionMap.get(sectionKey);
            if (sectionNode == null) {
                sectionNode = new LinkedHashMap<>();
                sectionNode.put("nodeType", "section");
                sectionNode.put("chapterIndex", chapterIndex);
                sectionNode.put("sectionIndex", sectionIndex);
                sectionNode.put("title", sectionTitle);
                sectionNode.put("children", new ArrayList<Map<String, Object>>());
                sectionMap.put(sectionKey, sectionNode);
                chapterChildren.add(sectionNode);
            }

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> sectionChildren = (List<Map<String, Object>>) sectionNode.get("children");
            Map<String, Object> leaf = new LinkedHashMap<>();
            leaf.put("nodeType", "leaf");
            leaf.put("episodeNo", episodeNo);
            leaf.put("chapterIndex", chapterIndex);
            leaf.put("sectionIndex", sectionIndex);
            leaf.put("subSectionIndex", subSectionIndex);
            leaf.put("outlineIndex", asText(episode.get("outlineIndex")));
            leaf.put("title", firstNonBlank(asText(episode.get("title")), "条目 " + Math.max(1, episodeNo)));
            leaf.put("startPage", asInt(episode.get("startPage"), -1));
            leaf.put("endPage", asInt(episode.get("endPage"), -1));
            sectionChildren.add(leaf);
        }
        return tree;
    }

    private int resolveBookStartPage(Integer pageOffset, List<Map<String, Object>> sections, int totalPages) {
        int fromOffset = pageOffset != null ? pageOffset + 1 : -1;
        int fallback = findFirstSectionStartPage(sections);
        int candidate = fromOffset > 0 ? fromOffset : fallback;
        if (candidate <= 0) {
            candidate = 1;
        }
        return clampPage(candidate, totalPages);
    }

    private int findFirstSectionStartPage(List<Map<String, Object>> sections) {
        if (sections == null || sections.isEmpty()) {
            return -1;
        }
        for (Map<String, Object> section : sections) {
            if (section == null) {
                continue;
            }
            int startPage = asInt(section.get("startPage"), -1);
            if (startPage > 0) {
                return startPage;
            }
        }
        return -1;
    }

    private int clampPage(int pageNo, int totalPages) {
        int normalized = Math.max(1, pageNo);
        if (totalPages > 0) {
            return Math.min(totalPages, normalized);
        }
        return normalized;
    }

    private String chooseVideoInfoProbeInput(String rawInput, String normalizedInput) {
        String raw = rawInput != null ? rawInput.trim() : "";
        String normalized = normalizedInput != null ? normalizedInput.trim() : "";
        String lowerRaw = raw.toLowerCase(Locale.ROOT);
        if (lowerRaw.contains("http://") || lowerRaw.contains("https://")) {
            return raw;
        }
        if (!normalized.isEmpty()) {
            return normalized;
        }
        return raw;
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

    private TaskQueueManager.BookProcessingOptions buildBookProcessingOptions(
            String chapterSelector,
            String sectionSelector,
            Boolean splitByChapter,
            Boolean splitBySection,
            Integer pageOffset
    ) {
        String normalizedChapterSelector = normalizeOptionalText(chapterSelector);
        String normalizedSectionSelector = normalizeOptionalText(sectionSelector);
        if (normalizedChapterSelector == null
                && normalizedSectionSelector == null
                && splitByChapter == null
                && splitBySection == null
                && pageOffset == null) {
            return null;
        }
        TaskQueueManager.BookProcessingOptions options = new TaskQueueManager.BookProcessingOptions();
        options.chapterSelector = normalizedChapterSelector;
        options.sectionSelector = normalizedSectionSelector;
        options.splitByChapter = splitByChapter;
        options.splitBySection = splitBySection;
        options.pageOffset = pageOffset;
        return options;
    }

    private String normalizeOptionalText(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
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

    private boolean hasSupportedUploadExtension(String fileName) {
        String lower = fileName.toLowerCase(Locale.ROOT);
        for (String ext : ALLOWED_UPLOAD_EXTENSIONS) {
            if (lower.endsWith(ext)) {
                return true;
            }
        }
        return false;
    }

    private Path persistUploadedVideo(MultipartFile videoFile, String safeFileName) throws IOException {
        Path uploadRootPath = fileTransferService.resolveTransferRoot(uploadDir);
        return fileTransferService.persistMultipartWithUniqueName(uploadRootPath, safeFileName, videoFile);
    }
}
