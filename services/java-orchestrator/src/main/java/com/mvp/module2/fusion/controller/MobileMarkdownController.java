package com.mvp.module2.fusion.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mvp.module2.fusion.common.TaskDisplayNameResolver;
import com.mvp.module2.fusion.common.UserFacingErrorMapper;
import com.mvp.module2.fusion.common.VideoInputNormalizer;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskStatus;
import com.mvp.module2.fusion.service.CollectionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRange;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.HashSet;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Mobile Markdown API.
 * Design goal: reuse TaskQueueManager, aggregate runtime/storage task views, and enforce strict path normalization and boundary checks. */
@RestController
@RequestMapping("/api/mobile")
public class MobileMarkdownController {

    private static final Logger logger = LoggerFactory.getLogger(MobileMarkdownController.class);
    private static final String STORAGE_TASK_PREFIX = "storage:";
    private static final String DEFAULT_MARKDOWN_NAME = "enhanced_output.md";
    private static final int MARKDOWN_SCAN_DEPTH = 4;
    private static final String META_FILE_NAME = "mobile_task_meta.json";
    private static final String TELEMETRY_FILE_NAME = "mobile_task_telemetry.ndjson";
    private static final String META_DEFAULT_NOTE_KEY = "__default__";
    private static final Pattern UNSAFE_FILENAME_CHARS = Pattern.compile("[^A-Za-z0-9._-]");
    private static final Set<String> ALLOWED_VIDEO_EXTENSIONS = Set.of(".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v");
    private static final long MAX_UPLOAD_FILE_BYTES = 2L * 1024L * 1024L * 1024L;

    @Autowired
    private TaskQueueManager taskQueueManager;

    @Autowired
    private com.mvp.module2.fusion.service.StorageTaskCacheService storageTaskCacheService;

    @Autowired(required = false)
    private com.mvp.module2.fusion.service.PersonaAwareReadingService personaAwareReadingService;

    @Autowired(required = false)
    private com.mvp.module2.fusion.service.PersonaInsightCardService personaInsightCardService;

    @Autowired(required = false)
    private CollectionRepository collectionRepository;

    @Value("${task.upload.dir:var/uploads}")
    private String uploadDir;

    private final ObjectMapper objectMapper = new ObjectMapper();
    @GetMapping("/tasks")
    public ResponseEntity<Map<String, Object>> listTasks(
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "pageSize", defaultValue = "20") int pageSize,
            @RequestParam(value = "onlyMultiSegment", defaultValue = "true") boolean onlyMultiSegment
    ) {
        List<TaskEntry> runtimeTasks = taskQueueManager.getAllTasks();
        com.mvp.module2.fusion.service.StorageTaskCacheService.PagedResult storageResult =
                storageTaskCacheService.getTasks(page, pageSize);

        List<TaskView> finalViewList = new ArrayList<>();
        if (page == 0) {
            for (TaskEntry runtimeTask : runtimeTasks) {
                finalViewList.add(fromRuntimeTask(runtimeTask));
            }
        }

        for (com.mvp.module2.fusion.service.StorageTaskCacheService.CachedTask cached : storageResult.tasks) {
            finalViewList.add(fromCachedTask(cached));
        }
        finalViewList.sort(Comparator.comparingLong(this::bestTimestamp).reversed());
        Map<String, CollectionRepository.EpisodeTaskBinding> bindingByTaskId = findCollectionBindingByTaskId(finalViewList);

        List<Map<String, Object>> taskList = new ArrayList<>(finalViewList.size());
        for (TaskView task : finalViewList) {
            attachCollectionBinding(task, bindingByTaskId.get(task.taskId));
            if (onlyMultiSegment && !isTaskMultiSegmentReadable(task)) {
                continue;
            }
            taskList.add(toListItem(task));
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("tasks", taskList);
        if (onlyMultiSegment) {
            response.put("totalCount", taskList.size());
        } else {
            response.put("totalCount", runtimeTasks.size() + storageResult.totalCount);
        }
        response.put("page", page);
        response.put("pageSize", pageSize);
        response.put("hasMore", storageResult.hasMore);

        return ResponseEntity.ok(response);
    }

    @PostMapping("/tasks/submit")
    public ResponseEntity<Map<String, Object>> submitTaskFromMobile(@RequestBody TaskSubmitRequest request) {
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "request body cannot be empty"
            ));
        }

        String normalizedVideoInput = normalizeVideoInput(request.videoUrl);
        if (normalizedVideoInput.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "videoUrl cannot be empty"
            ));
        }
        if (!isCollectionInputValid(request.collectionId, request.episodeNo)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "collectionId and episodeNo must be provided together, and episodeNo must be positive"
            ));
        }

        String normalizedUserId = normalizeUserId(request.userId);
        TaskQueueManager.Priority priority = resolvePriority(normalizedUserId, request.priority);
        logger.info("Mobile task submission: raw={} normalized={} user={}", request.videoUrl, normalizedVideoInput, normalizedUserId);
        TaskQueueManager.TaskEntry task = taskQueueManager.submitTask(
                normalizedUserId,
                normalizedVideoInput,
                normalizeOutputDir(request.outputDir),
                priority
        );
        linkCollectionEpisodeIfNecessary(request.collectionId, request.episodeNo, task.taskId);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("taskId", task.taskId);
        payload.put("status", task.status.name());
        payload.put("normalizedVideoUrl", normalizedVideoInput);
        payload.put("collectionId", request.collectionId != null ? request.collectionId.trim() : "");
        payload.put("episodeNo", request.episodeNo);
        payload.put("message", "task submitted and queued");
        return ResponseEntity.ok(payload);
    }

    @GetMapping("/collections")
    public ResponseEntity<Map<String, Object>> listCollections() {
        if (collectionRepository == null) {
            return ResponseEntity.ok(Map.of("collections", List.of()));
        }
        List<CollectionRepository.CollectionView> collections = collectionRepository.listCollections();
        Map<String, TaskEntry> runtimeTaskById = new LinkedHashMap<>();
        for (TaskEntry runtimeTask : taskQueueManager.getAllTasks()) {
            if (runtimeTask == null || runtimeTask.taskId == null || runtimeTask.taskId.isBlank()) {
                continue;
            }
            runtimeTaskById.put(runtimeTask.taskId, runtimeTask);
        }

        List<Map<String, Object>> collectionItems = new ArrayList<>(collections.size());
        for (CollectionRepository.CollectionView collection : collections) {
            List<CollectionRepository.EpisodeView> episodes = collectionRepository.listEpisodes(collection.collectionId);
            List<Map<String, Object>> episodeItems = new ArrayList<>(episodes.size());
            int completedCount = 0;
            for (CollectionRepository.EpisodeView episode : episodes) {
                String taskId = episode.taskId != null ? episode.taskId.trim() : "";
                String status = null;
                if (!taskId.isEmpty()) {
                    TaskEntry runtimeTask = runtimeTaskById.get(taskId);
                    if (runtimeTask != null && runtimeTask.status != null) {
                        status = runtimeTask.status.name();
                        if (runtimeTask.status == TaskStatus.COMPLETED) {
                            completedCount += 1;
                        }
                    }
                }
                Map<String, Object> episodeItem = new LinkedHashMap<>();
                episodeItem.put("episodeNo", episode.episodeNo);
                episodeItem.put("title", episode.episodeTitle != null ? episode.episodeTitle : "");
                episodeItem.put("episodeUrl", episode.episodeUrl != null ? episode.episodeUrl : "");
                episodeItem.put("durationSec", episode.durationSec);
                episodeItem.put("taskId", taskId.isEmpty() ? null : taskId);
                episodeItem.put("status", status);
                episodeItems.add(episodeItem);
            }

            Map<String, Object> collectionItem = new LinkedHashMap<>();
            collectionItem.put("collectionId", collection.collectionId);
            collectionItem.put("platform", collection.platform != null ? collection.platform : "");
            collectionItem.put("canonicalId", collection.canonicalId != null ? collection.canonicalId : "");
            collectionItem.put("title", collection.title != null ? collection.title : "");
            collectionItem.put("totalEpisodes", collection.totalEpisodes);
            collectionItem.put("completedCount", completedCount);
            collectionItem.put("episodes", episodeItems);
            collectionItems.add(collectionItem);
        }

        return ResponseEntity.ok(Map.of("collections", collectionItems));
    }

    @PostMapping("/collections/{collectionId}/submit-batch")
    public ResponseEntity<Map<String, Object>> submitCollectionBatch(
            @PathVariable("collectionId") String collectionId,
            @RequestBody(required = false) CollectionBatchSubmitRequest request
    ) {
        if (collectionRepository == null) {
            return ResponseEntity.status(503).body(Map.of(
                    "success", false,
                    "message", "collection repository is not available"
            ));
        }
        String normalizedCollectionId = collectionId != null ? collectionId.trim() : "";
        if (normalizedCollectionId.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "collectionId cannot be empty"
            ));
        }
        Optional<CollectionRepository.CollectionView> collectionOpt = collectionRepository.findCollection(normalizedCollectionId);
        if (collectionOpt.isEmpty()) {
            return ResponseEntity.status(404).body(Map.of(
                    "success", false,
                    "message", "collection not found"
            ));
        }

        List<CollectionRepository.EpisodeView> allEpisodes = collectionRepository.listEpisodes(normalizedCollectionId);
        if (allEpisodes.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "collection episodes are empty"
            ));
        }

        List<Integer> requestedEpisodeNos = request != null ? request.episodeNos : null;
        Set<Integer> selectedNos = new LinkedHashSet<>();
        if (requestedEpisodeNos != null) {
            for (Integer episodeNo : requestedEpisodeNos) {
                if (episodeNo == null || episodeNo <= 0) {
                    continue;
                }
                selectedNos.add(episodeNo);
            }
        }
        boolean submitAllUnlinked = selectedNos.isEmpty();
        String normalizedUserId = normalizeUserId(request != null ? request.userId : null);
        TaskQueueManager.Priority priority = resolvePriority(normalizedUserId, request != null ? request.priority : null);
        String normalizedOutputDir = normalizeOutputDir(request != null ? request.outputDir : null);

        List<Map<String, Object>> submitted = new ArrayList<>();
        List<Map<String, Object>> skipped = new ArrayList<>();
        for (CollectionRepository.EpisodeView episode : allEpisodes) {
            if (!submitAllUnlinked && !selectedNos.contains(episode.episodeNo)) {
                continue;
            }
            String existingTaskId = episode.taskId != null ? episode.taskId.trim() : "";
            if (!existingTaskId.isEmpty()) {
                skipped.add(Map.of(
                        "episodeNo", episode.episodeNo,
                        "title", episode.episodeTitle != null ? episode.episodeTitle : "",
                        "reason", "already linked",
                        "taskId", existingTaskId
                ));
                continue;
            }
            String normalizedVideoInput = normalizeVideoInput(episode.episodeUrl);
            if (normalizedVideoInput.isBlank()) {
                skipped.add(Map.of(
                        "episodeNo", episode.episodeNo,
                        "title", episode.episodeTitle != null ? episode.episodeTitle : "",
                        "reason", "episodeUrl is empty or invalid"
                ));
                continue;
            }

            TaskEntry task = taskQueueManager.submitTask(
                    normalizedUserId,
                    normalizedVideoInput,
                    normalizedOutputDir,
                    priority
            );
            linkCollectionEpisodeIfNecessary(normalizedCollectionId, episode.episodeNo, task.taskId);
            submitted.add(Map.of(
                    "episodeNo", episode.episodeNo,
                    "title", episode.episodeTitle != null ? episode.episodeTitle : "",
                    "taskId", task.taskId,
                    "status", task.status != null ? task.status.name() : "",
                    "normalizedVideoUrl", normalizedVideoInput
            ));
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("collectionId", normalizedCollectionId);
        payload.put("submittedCount", submitted.size());
        payload.put("skippedCount", skipped.size());
        payload.put("submitted", submitted);
        payload.put("skipped", skipped);
        payload.put("message", "batch submission finished");
        return ResponseEntity.ok(payload);
    }

    @PostMapping(value = "/tasks/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> submitUploadTaskFromMobile(
            @RequestParam("videoFile") MultipartFile videoFile,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam(value = "outputDir", required = false) String outputDir,
            @RequestParam(value = "priority", required = false) String priority
    ) {
        if (videoFile == null || videoFile.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "videoFile cannot be empty"
            ));
        }
        if (videoFile.getSize() > MAX_UPLOAD_FILE_BYTES) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "uploaded file is too large; current limit is 2048MB"
            ));
        }

        String safeFileName = sanitizeUploadFileName(videoFile.getOriginalFilename());
        if (!hasSupportedVideoExtension(safeFileName)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "supported video formats: mp4/mov/mkv/avi/webm/m4v"
            ));
        }

        String normalizedUserId = normalizeUserId(userId);
        TaskQueueManager.Priority taskPriority = resolvePriority(normalizedUserId, priority);

        try {
            Path savedVideoPath = persistUploadedVideo(videoFile, safeFileName);
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
                    "message", "video uploaded; task submitted and queued"
            ));
        } catch (IOException e) {
            logger.error("mobile upload video persistence failed", e);
            return ResponseEntity.status(503).body(Map.of(
                    "success", false,
                    "message", UserFacingErrorMapper.busyMessage()
            ));
        }
    }

    @GetMapping("/tasks/{taskId}/markdown")
    public ResponseEntity<?> getTaskMarkdown(
            @PathVariable String taskId,
            @RequestParam(value = "userId", required = false) String userId
    ) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "task not found"));
        }

        ResolvedMarkdown resolved;
        try {
            resolved = resolveMarkdown(task);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.status(404).body(Map.of("message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("read markdown failed: taskId={} err={}", taskId, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "read markdown failed"));
        }

        try {
            Instant openedAt = markTaskOpened(task);
            String markdown = Files.readString(resolved.markdownPath, StandardCharsets.UTF_8);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("taskId", task.taskId);
            response.put("title", task.title);
            response.put("status", task.status);
            response.put("lastOpenedAt", instantToText(openedAt));
            response.put("markdown", markdown);
            response.put("markdownPath", resolved.markdownPath.toString());
            response.put("baseDir", resolved.baseDir.toString());
            response.put("assetEndpointTemplate", "/api/mobile/tasks/" + task.taskId + "/asset?path={path}");
            appendPersonalizedReading(
                    response,
                    task.taskId,
                    resolveReaderUserId(task, userId),
                    resolved.markdownPath,
                    markdown
            );
            return ResponseEntity.ok(response);
        } catch (IOException ex) {
            logger.warn("read markdown content failed: taskId={} path={} err={}", taskId, resolved.markdownPath, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "read markdown content failed"));
        }
    }

    @GetMapping("/tasks/{taskId}/markdown/by-path")
    public ResponseEntity<?> getTaskMarkdownByRelativePath(
            @PathVariable String taskId,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam("path") String rawPath
    ) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "task not found"));
        }
        if (rawPath == null || rawPath.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("message", "missing path parameter"));
        }

        ResolvedMarkdown resolved;
        try {
            resolved = resolveMarkdown(task);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.status(404).body(Map.of("message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("?? markdown ??: taskId={} err={}", taskId, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "?? markdown ??"));
        }

        String decodedPath = URLDecoder.decode(rawPath, StandardCharsets.UTF_8);
        Path target = resolveAssetTargetPath(resolved.baseDir, decodedPath);
        if (!target.startsWith(resolved.baseDir)) {
            return ResponseEntity.status(400).body(Map.of("message", "illegal path"));
        }
        if (!Files.exists(target) || !Files.isRegularFile(target)) {
            return ResponseEntity.status(404).body(Map.of("message", "file not found"));
        }
        if (!isMarkdownFile(target.getFileName().toString())) {
            return ResponseEntity.status(400).body(Map.of("message", "target is not a markdown file"));
        }

        try {
            Instant openedAt = markTaskOpened(task);
            String markdown = Files.readString(target, StandardCharsets.UTF_8);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("taskId", task.taskId);
            response.put("title", target.getFileName().toString());
            response.put("status", task.status);
            response.put("lastOpenedAt", instantToText(openedAt));
            response.put("markdown", markdown);
            response.put("markdownPath", target.toString());
            response.put("baseDir", resolved.baseDir.toString());
            response.put("assetEndpointTemplate", "/api/mobile/tasks/" + task.taskId + "/asset?path={path}");
            appendPersonalizedReading(
                    response,
                    task.taskId,
                    resolveReaderUserId(task, userId),
                    target,
                    markdown
            );
            return ResponseEntity.ok(response);
        } catch (IOException ex) {
            logger.warn("read relative markdown failed: taskId={} path={} err={}", taskId, rawPath, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "?? markdown ??"));
        }
    }

    @PostMapping("/tasks/{taskId}/opened")
    public ResponseEntity<?> markTaskOpenedTimestamp(@PathVariable String taskId) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "task not found"));
        }
        Instant openedAt = markTaskOpened(task);
        if (openedAt == null) {
            return ResponseEntity.status(500).body(Map.of("message", "update task open timestamp failed"));
        }
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("taskId", task.taskId);
        payload.put("title", task.title != null ? task.title : task.taskId);
        payload.put("lastOpenedAt", instantToText(openedAt));
        return ResponseEntity.ok(payload);
    }

    @GetMapping("/tasks/{taskId}")
    public ResponseEntity<?> getTaskRuntimeStatus(@PathVariable String taskId) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "task not found"));
        }
        Map<String, Object> response = new LinkedHashMap<>();
        String displayTitle = task.title != null && !task.title.isBlank() ? task.title : task.taskId;
        response.put("taskId", task.taskId);
        response.put("title", displayTitle);
        response.put("status", task.status != null ? task.status : "");
        response.put("progress", task.progress);
        response.put("statusMessage", task.statusMessage != null ? task.statusMessage : "");
        response.put("createdAt", instantToText(task.createdAt));
        response.put("completedAt", instantToText(task.completedAt));
        response.put("markdownAvailable", task.markdownAvailable);
        return ResponseEntity.ok(response);
    }

    @DeleteMapping("/tasks/{taskId}")
    public ResponseEntity<Map<String, Object>> cancelRuntimeTask(@PathVariable String taskId) {
        TaskEntry runtimeTask = taskQueueManager.getTask(taskId);
        if (runtimeTask == null) {
            return ResponseEntity.status(404).body(Map.of(
                    "success", false,
                    "status", "NOT_FOUND",
                    "message", "task not found"
            ));
        }

        TaskStatus currentStatus = runtimeTask.status != null ? runtimeTask.status : TaskStatus.QUEUED;
        if (currentStatus == TaskStatus.COMPLETED
                || currentStatus == TaskStatus.FAILED
                || currentStatus == TaskStatus.CANCELLED) {
            return ResponseEntity.status(409).body(Map.of(
                    "success", false,
                    "status", currentStatus.name(),
                    "message", "task is already finished"
            ));
        }

        boolean cancelled = taskQueueManager.cancelTask(taskId);
        if (!cancelled) {
            return ResponseEntity.status(409).body(Map.of(
                    "success", false,
                    "status", currentStatus.name(),
                    "message", "task cannot be cancelled in current state"
            ));
        }

        return ResponseEntity.ok(Map.of(
                "success", true,
                "status", TaskStatus.CANCELLED.name(),
                "message", "task cancelled and no new steps will be scheduled"
        ));
    }

    @GetMapping("/tasks/{taskId}/personalization/cache")
    public ResponseEntity<?> inspectTaskPersonalizationCache(
            @PathVariable String taskId,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam(value = "path", required = false) String rawPath
    ) {
        if (personaAwareReadingService == null) {
            return ResponseEntity.status(503).body(Map.of("message", "persona reading service unavailable"));
        }

        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "task not found"));
        }

        ResolvedMarkdown resolved;
        try {
            resolved = resolveMarkdown(task);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.status(404).body(Map.of("message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("inspect personalization cache failed: taskId={} err={}", taskId, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "read markdown failed"));
        }

        Path target = resolved.markdownPath;
        if (rawPath != null && !rawPath.isBlank()) {
            String decodedPath = URLDecoder.decode(rawPath, StandardCharsets.UTF_8);
            target = resolveAssetTargetPath(resolved.baseDir, decodedPath);
            if (!target.startsWith(resolved.baseDir)) {
                return ResponseEntity.status(400).body(Map.of("message", "invalid path"));
            }
            if (!Files.exists(target) || !Files.isRegularFile(target)) {
                return ResponseEntity.status(404).body(Map.of("message", "file not found"));
            }
            if (!isMarkdownFile(target.getFileName().toString())) {
                return ResponseEntity.status(400).body(Map.of("message", "target is not markdown"));
            }
        }

        String resolvedUserId = resolveReaderUserId(task, userId);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("taskId", task.taskId);
        payload.put("resolvedUserId", resolvedUserId != null ? resolvedUserId : "");
        payload.put("inspectedMarkdownPath", target.toString());
        payload.putAll(personaAwareReadingService.inspectCache(task.taskId, resolvedUserId, target));
        return ResponseEntity.ok(payload);
    }


    @PutMapping("/tasks/{taskId}/markdown")
    public ResponseEntity<?> updateTaskMarkdown(
            @PathVariable String taskId,
            @RequestBody MarkdownUpdateRequest request
    ) {
        if (request == null || request.markdown == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "?? markdown ??"));
        }

        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "task not found"));
        }

        ResolvedMarkdown resolved;
        try {
            resolved = resolveMarkdown(task);
        } catch (Exception ex) {
            return ResponseEntity.status(404).body(Map.of("message", "editable markdown not found"));
        }

        Path target = resolved.markdownPath;
        if (request.path != null && !request.path.isBlank()) {
            String decodedPath = URLDecoder.decode(request.path, StandardCharsets.UTF_8);
            target = resolveAssetTargetPath(resolved.baseDir, decodedPath);
            if (!target.startsWith(resolved.baseDir)) {
                return ResponseEntity.status(400).body(Map.of("message", "illegal path"));
            }
            if (!isMarkdownFile(target.getFileName().toString())) {
                return ResponseEntity.status(400).body(Map.of("message", "target is not a markdown file"));
            }
        }

        try {
            if (target.getParent() != null) {
                Files.createDirectories(target.getParent());
            }
            Files.writeString(target, request.markdown, StandardCharsets.UTF_8);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("taskId", task.taskId);
            payload.put("markdownPath", target.toString());
            payload.put("baseDir", target.getParent() != null ? target.getParent().toString() : "");
            payload.put("size", request.markdown.length());
            payload.put("updatedAt", Instant.now().toString());
            return ResponseEntity.ok(payload);
        } catch (IOException ex) {
            logger.warn("write markdown failed: taskId={} path={} err={}", taskId, target, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "write markdown failed"));
        }
    }

    @GetMapping("/tasks/{taskId}/meta")
    public ResponseEntity<?> getTaskMeta(
            @PathVariable String taskId,
            @RequestParam(value = "path", required = false) String rawPath
    ) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "task not found"));
        }
        Path taskRoot;
        try {
            taskRoot = resolveTaskRootDir(task);
        } catch (Exception ex) {
            return ResponseEntity.status(404).body(Map.of("message", "task directory not found"));
        }

        String noteKey = normalizeMetaNoteKey(taskRoot, rawPath);
        TaskMetaFile meta = readTaskMeta(taskRoot);
        NoteMeta noteMeta = meta.notesByMarkdown.getOrDefault(noteKey, new NoteMeta());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("taskId", task.taskId);
        payload.put("taskTitle", meta.taskTitle != null ? meta.taskTitle : "");
        payload.put("lastOpenedAt", meta.lastOpenedAt != null ? meta.lastOpenedAt : "");
        payload.put("pathKey", noteKey);
        payload.put("favorites", noteMeta.favorites != null ? noteMeta.favorites : Map.of());
        payload.put("deleted", noteMeta.deleted != null ? noteMeta.deleted : Map.of());
        payload.put("comments", noteMeta.comments != null ? sanitizeComments(noteMeta.comments) : Map.of());
        payload.put("tokenLike", noteMeta.tokenLike != null ? noteMeta.tokenLike : Map.of());
        payload.put("tokenAnnotations", noteMeta.tokenAnnotations != null ? sanitizeTokenAnnotations(noteMeta.tokenAnnotations) : Map.of());
        payload.put("metaPath", taskRoot.resolve(META_FILE_NAME).toString());
        return ResponseEntity.ok(payload);
    }

    @PutMapping("/tasks/{taskId}/meta")
    public ResponseEntity<?> updateTaskMeta(
            @PathVariable String taskId,
            @RequestBody TaskMetaUpdateRequest request
    ) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "task not found"));
        }
        Path taskRoot;
        try {
            taskRoot = resolveTaskRootDir(task);
        } catch (Exception ex) {
            return ResponseEntity.status(404).body(Map.of("message", "task directory not found"));
        }

        TaskMetaFile meta = readTaskMeta(taskRoot);
        String normalizedTaskTitle = null;
        if (request != null && request.taskTitle != null) {
            String title = request.taskTitle.trim();
            meta.taskTitle = title.isEmpty() ? null : title;
            normalizedTaskTitle = meta.taskTitle;
        }

        String noteKey = normalizeMetaNoteKey(taskRoot, request != null ? request.path : null);
        NoteMeta noteMeta = meta.notesByMarkdown.getOrDefault(noteKey, new NoteMeta());
        if (request != null && request.favorites != null) {
            noteMeta.favorites = sanitizeFavorites(request.favorites);
        }
        if (request != null && request.deleted != null) {
            noteMeta.deleted = sanitizeDeleted(request.deleted);
        }
        if (request != null && request.comments != null) {
            noteMeta.comments = new LinkedHashMap<>(sanitizeComments(request.comments));
        }
        if (request != null && request.tokenLike != null) {
            noteMeta.tokenLike = sanitizeTokenLike(request.tokenLike);
        }
        if (request != null && request.tokenAnnotations != null) {
            noteMeta.tokenAnnotations = new LinkedHashMap<>(sanitizeTokenAnnotations(request.tokenAnnotations));
        }
        if ((noteMeta.favorites == null || noteMeta.favorites.isEmpty())
                && (noteMeta.deleted == null || noteMeta.deleted.isEmpty())
                && (noteMeta.comments == null || noteMeta.comments.isEmpty())
                && (noteMeta.tokenLike == null || noteMeta.tokenLike.isEmpty())
                && (noteMeta.tokenAnnotations == null || noteMeta.tokenAnnotations.isEmpty())) {
            meta.notesByMarkdown.remove(noteKey);
        } else {
            meta.notesByMarkdown.put(noteKey, noteMeta);
        }

        if (!writeTaskMeta(taskRoot, meta)) {
            return ResponseEntity.status(500).body(Map.of("message", "write task metadata failed"));
        }
        if (request != null && request.taskTitle != null) {
            if (!syncVideoMetaTitle(taskRoot, normalizedTaskTitle)) {
                return ResponseEntity.status(500).body(Map.of("message", "write video metadata failed"));
            }
        }
        if (normalizedTaskTitle != null) {
            task.title = normalizedTaskTitle;
            task.metaTitle = normalizedTaskTitle;
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("taskId", task.taskId);
        payload.put("taskTitle", meta.taskTitle != null ? meta.taskTitle : "");
        payload.put("lastOpenedAt", meta.lastOpenedAt != null ? meta.lastOpenedAt : "");
        payload.put("pathKey", noteKey);
        payload.put("favorites", noteMeta.favorites != null ? noteMeta.favorites : Map.of());
        payload.put("deleted", noteMeta.deleted != null ? noteMeta.deleted : Map.of());
        payload.put("comments", noteMeta.comments != null ? sanitizeComments(noteMeta.comments) : Map.of());
        payload.put("tokenLike", noteMeta.tokenLike != null ? noteMeta.tokenLike : Map.of());
        payload.put("tokenAnnotations", noteMeta.tokenAnnotations != null ? sanitizeTokenAnnotations(noteMeta.tokenAnnotations) : Map.of());
        payload.put("metaPath", taskRoot.resolve(META_FILE_NAME).toString());
        payload.put("updatedAt", Instant.now().toString());
        return ResponseEntity.ok(payload);
    }

    @PostMapping(value = "/tasks/{taskId}/telemetry", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> ingestTaskTelemetry(
            @PathVariable String taskId,
            @RequestBody TaskTelemetryIngestRequest request
    ) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "task not found"));
        }
        Path taskRoot;
        try {
            taskRoot = resolveTaskRootDir(task);
        } catch (Exception ex) {
            return ResponseEntity.status(404).body(Map.of("message", "task directory not found"));
        }
        if (request == null || request.events == null || request.events.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "missing telemetry events"));
        }

        String noteKey = normalizeMetaNoteKey(taskRoot, request.path);
        Path telemetryPath = taskRoot.resolve(TELEMETRY_FILE_NAME).normalize();
        if (!telemetryPath.startsWith(taskRoot)) {
            return ResponseEntity.badRequest().body(Map.of("message", "telemetry path is illegal"));
        }

        List<Map<String, Object>> accepted = new ArrayList<>();
        for (TelemetryEventItem item : request.events) {
            if (item == null) continue;
            String eventType = trimToNullSafe(item.eventType);
            if (eventType == null) continue;
            String nodeId = trimToNullSafe(item.nodeId);

            Map<String, Object> record = new LinkedHashMap<>();
            record.put("taskId", task.taskId);
            record.put("pathKey", noteKey);
            record.put("nodeId", nodeId != null ? nodeId : "");
            record.put("eventType", eventType);
            record.put("relevanceScore", item.relevanceScore != null ? item.relevanceScore : 0.0d);
            record.put("timestampMs", item.timestampMs != null ? item.timestampMs : System.currentTimeMillis());
            record.put("ingestedAt", Instant.now().toString());
            record.put("payload", sanitizeTelemetryPayload(item.payload));
            accepted.add(record);
        }

        if (accepted.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "no valid telemetry events"));
        }

        try {
            Files.createDirectories(taskRoot);
            StringBuilder builder = new StringBuilder();
            for (Map<String, Object> record : accepted) {
                builder.append(objectMapper.writeValueAsString(record)).append('\n');
            }
            Files.writeString(
                    telemetryPath,
                    builder.toString(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND
            );

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("taskId", task.taskId);
            payload.put("pathKey", noteKey);
            payload.put("accepted", accepted.size());
            payload.put("telemetryPath", telemetryPath.toString());
            payload.put("updatedAt", Instant.now().toString());
            return ResponseEntity.ok(payload);
        } catch (Exception ex) {
            logger.warn("?? telemetry ??: taskId={} path={} err={}", taskId, telemetryPath, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "?? telemetry ??"));
        }
    }

    @GetMapping("/tasks/{taskId}/asset")
    public ResponseEntity<?> getTaskAsset(
            @PathVariable String taskId,
            @RequestParam("path") String rawPath,
            @RequestHeader(value = HttpHeaders.RANGE, required = false) String rangeHeader
    ) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "task not found"));
        }
        if (rawPath == null || rawPath.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("message", "?? path ??"));
        }

        ResolvedMarkdown resolved;
        try {
            resolved = resolveMarkdown(task);
        } catch (Exception ex) {
            return ResponseEntity.status(404).body(Map.of("message", "markdown root directory not found"));
        }

        String decodedPath = URLDecoder.decode(rawPath, StandardCharsets.UTF_8);
        Path target = resolveAssetTargetPath(resolved.baseDir, decodedPath);
        if (!target.startsWith(resolved.baseDir)) {
            return ResponseEntity.status(400).body(Map.of("message", "illegal path"));
        }
        if (!Files.exists(target) || !Files.isRegularFile(target)) {
            return ResponseEntity.status(404).body(Map.of("message", "file not found"));
        }

        try {
            long length = Files.size(target);
            MediaType mediaType = detectMediaType(target);

            if (rangeHeader != null && !rangeHeader.isBlank()) {
                try {
                    List<HttpRange> ranges = HttpRange.parseRanges(rangeHeader);
                    if (!ranges.isEmpty()) {
                        HttpRange range = ranges.get(0);
                        long start = range.getRangeStart(length);
                        long end = range.getRangeEnd(length);
                        long regionLength = end - start + 1;
                        InputStream fileStream = Files.newInputStream(target);
                        fileStream.skip(start);
                        InputStream bounded = new java.io.FilterInputStream(fileStream) {
                            long remaining = regionLength;
                            @Override public int read() throws IOException {
                                if (remaining <= 0) return -1;
                                int b = super.read();
                                if (b >= 0) remaining--;
                                return b;
                            }
                            @Override public int read(byte[] b, int off, int len) throws IOException {
                                if (remaining <= 0) return -1;
                                int n = super.read(b, off, (int) Math.min(len, remaining));
                                if (n > 0) remaining -= n;
                                return n;
                            }
                        };
                        Resource rangeResource = new InputStreamResource(bounded);
                        return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
                                .contentType(mediaType)
                                .header(HttpHeaders.ACCEPT_RANGES, "bytes")
                                .header(HttpHeaders.CONTENT_RANGE, "bytes " + start + "-" + end + "/" + length)
                                .contentLength(regionLength)
                                .body(rangeResource);
                    }
                } catch (Exception ex) {
                    logger.warn("range request processing failed, falling back to full response: taskId={} path={} range={} err={}",
                            taskId, target, rangeHeader, ex.getMessage());
                }
            }

            InputStream inputStream = Files.newInputStream(target);
            Resource resource = new InputStreamResource(inputStream);
            return ResponseEntity.ok()
                    .contentType(mediaType)
                    .contentLength(length)
                    .header(HttpHeaders.ACCEPT_RANGES, "bytes")
                    .header(HttpHeaders.CACHE_CONTROL, "public, max-age=60")
                    .header(HttpHeaders.CONTENT_DISPOSITION, "inline; filename=\"" + target.getFileName() + "\"")
                    .body(resource);
        } catch (IOException ex) {
            logger.warn("read resource file failed: taskId={} path={} err={}", taskId, target, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "read resource file failed"));
        }
    }

    @GetMapping("/tasks/{taskId}/export")
    public ResponseEntity<StreamingResponseBody> exportTaskBundle(@PathVariable String taskId) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "task not found");
        }
        Path taskRoot;
        try {
            taskRoot = resolveTaskRootDir(task);
        } catch (Exception ex) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "task directory not found", ex);
        }
        if (!Files.exists(taskRoot) || !Files.isDirectory(taskRoot)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "task directory does not exist");
        }

        final List<ExportFileEntry> exportEntries;
        try {
            exportEntries = collectExportEntries(taskRoot);
        } catch (IOException ex) {
            logger.warn("export task failed: taskId={} root={} err={}", taskId, taskRoot, ex.getMessage());
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "export task failed", ex);
        }
        if (exportEntries.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no exportable markdown/video/screenshot assets found");
        }

        String filename = buildSafeExportFilename(task.taskId);
        StreamingResponseBody body = outputStream -> {
            try (ZipOutputStream zos = new ZipOutputStream(outputStream, StandardCharsets.UTF_8)) {
                ExportZipResult zipResult = writeTaskExportZipStreaming(exportEntries, zos);
                zos.finish();
                logger.info("streaming export completed: taskId={} root={} exported={} skipped={}",
                        taskId, taskRoot, zipResult.exportedCount, zipResult.skippedCount);
            } catch (IOException ex) {
                logger.warn("streaming export failed: taskId={} root={} err={}", taskId, taskRoot, ex.getMessage());
                throw ex;
            }
        };
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + filename + "\"")
                .body(body);
    }
    private String buildSafeExportFilename(String taskId) {
        String raw = taskId == null ? "task" : taskId.trim();
        if (raw.isEmpty()) {
            raw = "task";
        }
        String safe = raw.replace(':', '_').replaceAll("[^A-Za-z0-9._-]", "_");
        if (safe.isEmpty()) {
            safe = "task";
        }
        return safe + "_bundle.zip";
    }

    private Map<String, Object> toListItem(TaskView task) {
        Map<String, Object> item = new LinkedHashMap<>();
        String displayTitle = task.title != null && !task.title.isBlank() ? task.title : task.taskId;
        item.put("taskId", task.taskId);
        item.put("title", displayTitle);
        item.put("metaTitle", task.metaTitle != null ? task.metaTitle : "");
        item.put("titleSource", resolveTaskTitleSource(task));
        item.put("videoUrl", task.videoUrl != null ? task.videoUrl : "");
        item.put("status", task.status != null ? task.status : "");
        item.put("createdAt", instantToText(task.createdAt));
        item.put("lastOpenedAt", instantToText(task.lastOpenedAt));
        item.put("completedAt", instantToText(task.completedAt));
        item.put("resultPath", task.resultPath != null ? task.resultPath : "");
        item.put("markdownPath", task.markdownPath != null ? task.markdownPath.toString() : "");
        item.put("markdownAvailable", task.markdownAvailable);
        item.put("source", task.storageTask ? "storage" : "runtime");
        item.put("storageKey", task.storageKey != null ? task.storageKey : "");
        item.put("progress", task.progress);
        item.put("statusMessage", task.statusMessage != null ? task.statusMessage : "");
        item.put("collectionId", task.collectionId != null ? task.collectionId : "");
        item.put("episodeNo", task.episodeNo);
        item.put("episodeTitle", task.episodeTitle != null ? task.episodeTitle : "");
        item.put("collectionTitle", task.collectionTitle != null ? task.collectionTitle : "");
        item.put("totalEpisodes", task.totalEpisodes);
        return item;
    }

    private Map<String, CollectionRepository.EpisodeTaskBinding> findCollectionBindingByTaskId(List<TaskView> tasks) {
        if (collectionRepository == null || tasks == null || tasks.isEmpty()) {
            return Map.of();
        }
        Set<String> taskIds = new LinkedHashSet<>();
        for (TaskView task : tasks) {
            if (task == null || task.taskId == null || task.taskId.isBlank()) {
                continue;
            }
            if (task.storageTask || task.taskId.startsWith(STORAGE_TASK_PREFIX)) {
                continue;
            }
            taskIds.add(task.taskId);
        }
        if (taskIds.isEmpty()) {
            return Map.of();
        }
        return collectionRepository.findEpisodeBindingsByTaskIds(taskIds);
    }

    private void attachCollectionBinding(TaskView task, CollectionRepository.EpisodeTaskBinding binding) {
        if (task == null || binding == null) {
            return;
        }
        task.collectionId = binding.collectionId;
        task.episodeNo = binding.episodeNo;
        task.episodeTitle = binding.episodeTitle;
        task.collectionTitle = binding.collectionTitle;
        task.totalEpisodes = binding.totalEpisodes;
    }

    private boolean isCollectionInputValid(String collectionId, Integer episodeNo) {
        boolean hasCollectionId = collectionId != null && !collectionId.trim().isEmpty();
        boolean hasEpisodeNo = episodeNo != null;
        if (hasCollectionId != hasEpisodeNo) {
            return false;
        }
        if (hasEpisodeNo) {
            return episodeNo > 0;
        }
        return true;
    }

    private void linkCollectionEpisodeIfNecessary(String collectionId, Integer episodeNo, String taskId) {
        if (!isCollectionInputValid(collectionId, episodeNo) || collectionRepository == null || episodeNo == null) {
            return;
        }
        boolean linked = collectionRepository.linkTaskToEpisode(collectionId.trim(), episodeNo, taskId);
        if (!linked) {
            logger.warn("link collection episode failed: collectionId={} episodeNo={} taskId={}",
                    collectionId, episodeNo, taskId);
        }
    }

    private boolean isTaskMultiSegmentReadable(TaskView task) {
        if (task == null || !task.markdownAvailable) {
            return false;
        }
        Path markdownPath = task.markdownPath;
        if (markdownPath == null) {
            try {
                ResolvedMarkdown resolved = resolveMarkdown(task);
                markdownPath = resolved.markdownPath;
            } catch (Exception ex) {
                return false;
            }
        }
        if (markdownPath == null || !Files.isRegularFile(markdownPath)) {
            return false;
        }
        return countMarkdownSegments(markdownPath, 2) >= 2;
    }

    private int countMarkdownSegments(Path markdownPath, int maxCount) {
        int segmentCount = 0;
        boolean inSegment = false;
        try (BufferedReader reader = Files.newBufferedReader(markdownPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    inSegment = false;
                    continue;
                }
                if (!inSegment) {
                    segmentCount += 1;
                    if (segmentCount >= maxCount) {
                        return segmentCount;
                    }
                    inSegment = true;
                }
            }
        } catch (IOException ex) {
            logger.debug("count markdown segments failed: path={} err={}", markdownPath, ex.getMessage());
            return 0;
        }
        return segmentCount;
    }

    private String resolveTaskTitleSource(TaskView task) {
        if (task == null) {
            return "taskId";
        }
        if (task.metaTitle != null && !task.metaTitle.isBlank()) {
            return "meta";
        }
        if (task.videoUrl != null && !task.videoUrl.isBlank()) {
            return "video";
        }
        String normalizedTitle = normalizeTaskTitleToken(task.title);
        String normalizedTaskId = normalizeTaskTitleToken(task.taskId);
        if (normalizedTitle != null) {
            if (normalizedTaskId != null && normalizedTitle.equals(normalizedTaskId)) {
                return "taskId";
            }
            if (isStorageFallbackTitle(task, normalizedTitle)) {
                return "taskId";
            }
            return "title";
        }
        return "taskId";
    }

    private String normalizeTaskTitleToken(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private boolean isStorageFallbackTitle(TaskView task, String normalizedTitle) {
        if (task == null || !task.storageTask || normalizedTitle == null) {
            return false;
        }
        String storageKey = normalizeTaskTitleToken(task.storageKey);
        if (storageKey != null && normalizedTitle.equals(storageKey)) {
            return true;
        }
        String taskId = normalizeTaskTitleToken(task.taskId);
        if (taskId == null) {
            return false;
        }
        if (normalizedTitle.equals(taskId)) {
            return true;
        }
        if (taskId.startsWith(STORAGE_TASK_PREFIX)) {
            return normalizedTitle.equals(taskId.substring(STORAGE_TASK_PREFIX.length()));
        }
        return false;
    }

    private TaskView resolveTaskView(String taskId) {
        if (taskId == null || taskId.isBlank()) {
            return null;
        }

        TaskEntry runtimeTask = taskQueueManager.getTask(taskId);
        if (runtimeTask != null) {
            return fromRuntimeTask(runtimeTask);
        }

        Optional<com.mvp.module2.fusion.service.StorageTaskCacheService.CachedTask> cachedByTaskIdOpt =
                storageTaskCacheService.getTaskByTaskId(taskId);
        if (cachedByTaskIdOpt.isPresent()) {
            TaskView view = fromCachedTask(cachedByTaskIdOpt.get());
            view.taskId = taskId;
            return view;
        }

        String storageKey = null;
        if (taskId.startsWith(STORAGE_TASK_PREFIX)) {
            storageKey = taskId.substring(STORAGE_TASK_PREFIX.length());
        } else if (isSafeStorageKey(taskId)) {
            // Backward compatibility: when storage: prefix is missing, fall back to resolving by directory name.
            storageKey = taskId;
        }
        if (storageKey == null || storageKey.isBlank()) {
            return null;
        }
        
        // Query task from storage cache service
        Optional<com.mvp.module2.fusion.service.StorageTaskCacheService.CachedTask> cachedOpt = 
                storageTaskCacheService.getTask(storageKey);
        return cachedOpt.map(this::fromCachedTask).orElse(null);
    }

    private String resolveReaderUserId(TaskView task, String requestUserId) {
        String fromRequest = trimToNullSafe(requestUserId);
        if (fromRequest != null) {
            return fromRequest;
        }
        if (task != null && task.runtimeTask != null) {
            String fromRuntime = trimToNullSafe(task.runtimeTask.userId);
            if (fromRuntime != null) {
                return fromRuntime;
            }
        }
        return null;
    }

    private void appendPersonalizedReading(
            Map<String, Object> response,
            String taskId,
            String userId,
            Path markdownPath,
            String markdown
    ) {
        if (personaAwareReadingService == null || response == null) {
            return;
        }
        try {
            com.mvp.module2.fusion.service.PersonaAwareReadingService.PersonalizedReadingPayload payload =
                    personaAwareReadingService.loadOrCompute(taskId, userId, markdownPath, markdown);
            if (payload == null) {
                return;
            }
            response.put("personalizedNodes", payload.nodes != null ? payload.nodes : List.of());
            response.put("personalizationSource", payload.source != null ? payload.source : "unknown");
            response.put("personalizationUserKey", payload.userKey != null ? payload.userKey : "");
            response.put("personalizationGeneratedAt", payload.generatedAt != null ? payload.generatedAt : "");
            response.put("personalizationCachePath", payload.cachePath != null ? payload.cachePath : "");
            response.put("personalizationCacheScope", payload.cacheScope != null ? payload.cacheScope : "");
            response.put("personalizationChunkStrategy", payload.chunkStrategy != null ? payload.chunkStrategy : "");
            response.put("personaProfile", payload.persona != null ? payload.persona : Map.of());
            if (personaInsightCardService != null) {
                Map<String, Object> insightIndex = personaInsightCardService.loadIndexSnapshot(taskId, markdownPath);
                if (!insightIndex.isEmpty()) {
                    response.put("insightCardIndex", insightIndex);
                }
            }
        } catch (Exception ex) {
            logger.warn("append personalized reading failed: taskId={} err={}", taskId, ex.getMessage());
        }
    }

    private TaskView fromRuntimeTask(TaskEntry task) {
        TaskView view = new TaskView();
        view.taskId = task.taskId;
        view.title = deriveTaskTitle(task.videoUrl, task.taskId);
        view.videoUrl = task.videoUrl;
        view.status = task.status != null ? task.status.name() : TaskStatus.QUEUED.name();
        view.createdAt = task.createdAt;
        view.completedAt = task.completedAt;
        view.resultPath = task.resultPath;
        view.progress = task.progress;
        view.statusMessage = task.statusMessage;
        view.runtimeTask = task;

        try {
            ResolvedMarkdown resolved = resolveMarkdown(task);
            view.markdownAvailable = true;
            view.markdownPath = resolved.markdownPath;
            view.baseDir = resolved.baseDir;
            view.taskRootDir = resolved.baseDir;
            if (view.resultPath == null || view.resultPath.isBlank()) {
                view.resultPath = resolved.markdownPath.toString();
            }
        } catch (Exception ignored) {
            view.markdownAvailable = false;
            if (task.resultPath != null && !task.resultPath.isBlank()) {
                try {
                    Path path = Paths.get(task.resultPath).toAbsolutePath().normalize();
                    if (Files.isDirectory(path)) {
                        view.taskRootDir = path;
                    } else if (Files.isRegularFile(path) && path.getParent() != null) {
                        view.taskRootDir = path.getParent();
                    }
                } catch (Exception ex) {
                    // Ignore fallback resolution errors and keep returning runtime task view.
                }
            }
        }

        applyTaskTitleFromMeta(view);

        return view;
    }

    private TaskView fromCachedTask(com.mvp.module2.fusion.service.StorageTaskCacheService.CachedTask cached) {
        TaskView view = new TaskView();
        view.taskId = STORAGE_TASK_PREFIX + cached.storageKey;
        view.storageKey = cached.storageKey;
        view.storageTask = true;
        view.title = cached.title;
        view.videoUrl = cached.videoUrl;
        view.status = cached.status;
        view.statusMessage = cached.statusMessage;
        view.createdAt = cached.createdAt;
        view.completedAt = cached.completedAt;
        view.resultPath = cached.resultPath;
        view.markdownAvailable = cached.markdownAvailable;
        view.markdownPath = cached.markdownPath;
        view.baseDir = cached.baseDir;
        view.taskRootDir = cached.taskRootDir;
        view.progress = cached.progress;
        
        applyTaskTitleFromMeta(view);
        return view;
    }

    private ResolvedMarkdown resolveMarkdown(TaskView task) throws IOException {
        if (task.markdownAvailable && task.markdownPath != null && task.baseDir != null) {
            return new ResolvedMarkdown(task.markdownPath, task.baseDir);
        }
        if (task.runtimeTask != null) {
            return resolveMarkdown(task.runtimeTask);
        }
        if (task.storageTask && task.storageKey != null) {
            Path taskDir = resolveStorageRoot().resolve(task.storageKey).normalize();
            if (Files.isDirectory(taskDir)) {
                return resolveMarkdownInDirectory(taskDir, task.resultPath);
            }
        }
        throw new IllegalArgumentException("markdown is not generated for this task yet");
    }

    private ResolvedMarkdown resolveMarkdown(TaskEntry task) throws IOException {
        if (task == null || task.resultPath == null || task.resultPath.isBlank()) {
            throw new IllegalArgumentException("markdown is not generated for this task yet");
        }
        Path resultPath = Paths.get(task.resultPath).toAbsolutePath().normalize();
        if (!Files.exists(resultPath)) {
            throw new IllegalArgumentException("result path does not exist: " + resultPath);
        }
        if (Files.isRegularFile(resultPath) && isMarkdownFile(resultPath.getFileName().toString())) {
            return new ResolvedMarkdown(resultPath, resultPath.getParent());
        }

        Path searchRoot = Files.isDirectory(resultPath)
                ? resultPath
                : Optional.ofNullable(resultPath.getParent()).orElse(resultPath);
        return resolveMarkdownInDirectory(searchRoot, null);
    }

    private ResolvedMarkdown resolveMarkdownInDirectory(Path searchRoot, String preferredPath) throws IOException {
        if (!Files.exists(searchRoot) || !Files.isDirectory(searchRoot)) {
            throw new IllegalArgumentException("no available markdown directory found");
        }

        if (preferredPath != null && !preferredPath.isBlank()) {
            try {
                Path preferred = Paths.get(preferredPath).toAbsolutePath().normalize();
                if (Files.isRegularFile(preferred) && preferred.startsWith(searchRoot) && isMarkdownFile(preferred.getFileName().toString())) {
                    return new ResolvedMarkdown(preferred, preferred.getParent());
                }
            } catch (Exception ignored) {
                // Fallback to directory scan.
            }
        }

        Path defaultMarkdown = searchRoot.resolve(DEFAULT_MARKDOWN_NAME);
        if (Files.isRegularFile(defaultMarkdown)) {
            Path normalized = defaultMarkdown.toAbsolutePath().normalize();
            return new ResolvedMarkdown(normalized, normalized.getParent());
        }

        List<Path> markdownFiles = new ArrayList<>();
        try (Stream<Path> stream = Files.walk(searchRoot, MARKDOWN_SCAN_DEPTH)) {
            stream.filter(Files::isRegularFile)
                    .filter(path -> isMarkdownFile(path.getFileName().toString()))
                    .filter(path -> !containsHiddenSegment(searchRoot, path))
                    .forEach(markdownFiles::add);
        }

        if (markdownFiles.isEmpty()) {
            throw new IllegalArgumentException("markdown file not found");
        }

        markdownFiles.sort(
                Comparator.comparingInt((Path path) -> markdownNamePriority(path.getFileName().toString()))
                        .thenComparing(Comparator.comparingLong(this::safeLastModifiedMillis).reversed())
        );
        Path selected = markdownFiles.get(0).toAbsolutePath().normalize();
        return new ResolvedMarkdown(selected, selected.getParent());
    }

    private boolean containsHiddenSegment(Path root, Path candidate) {
        Path relative;
        try {
            relative = root.toAbsolutePath().normalize().relativize(candidate.toAbsolutePath().normalize());
        } catch (Exception ex) {
            return false;
        }
        for (Path part : relative) {
            if (part.toString().startsWith(".")) {
                return true;
            }
        }
        return false;
    }

    private Path resolveAssetTargetPath(Path baseDir, String decodedPath) {
        String normalized = decodedPath == null ? "" : decodedPath.trim();
        while (normalized.startsWith("/") || normalized.startsWith("\\")) {
            normalized = normalized.substring(1);
        }
        return baseDir.resolve(normalized).normalize();
    }

    private Path resolveTaskRootDir(TaskView task) throws IOException {
        if (task == null) {
            throw new IllegalArgumentException("task is null");
        }
        if (task.taskRootDir != null) {
            return task.taskRootDir;
        }
        if (task.storageTask && task.storageKey != null) {
            Path root = resolveStorageRoot().resolve(task.storageKey).normalize();
            if (Files.isDirectory(root)) {
                task.taskRootDir = root;
                return root;
            }
        }
        if (task.runtimeTask != null && task.runtimeTask.resultPath != null && !task.runtimeTask.resultPath.isBlank()) {
            Path result = Paths.get(task.runtimeTask.resultPath).toAbsolutePath().normalize();
            if (Files.isDirectory(result)) {
                task.taskRootDir = result;
                return result;
            }
            if (Files.isRegularFile(result) && result.getParent() != null) {
                task.taskRootDir = result.getParent();
                return result.getParent();
            }
        }
        ResolvedMarkdown resolved = resolveMarkdown(task);
        task.taskRootDir = resolved.baseDir;
        return resolved.baseDir;
    }

    private TaskMetaFile readTaskMeta(Path taskRoot) {
        TaskMetaFile fallback = new TaskMetaFile();
        if (taskRoot == null) {
            return fallback;
        }
        Path metaPath = taskRoot.resolve(META_FILE_NAME).normalize();
        if (!metaPath.startsWith(taskRoot) || !Files.isRegularFile(metaPath)) {
            return fallback;
        }
        try {
            if (Files.size(metaPath) == 0L) {
                // Empty metadata file usually indicates interrupted historical write; treat as corrupt and rebuild.
                logger.warn("task metadata file is empty; rebuilding from fallback: {}", metaPath);
                Files.deleteIfExists(metaPath);
                return fallback;
            }
            TaskMetaFile loaded = objectMapper.readValue(metaPath.toFile(), TaskMetaFile.class);
            if (loaded == null) {
                return fallback;
            }
            if (loaded.notesByMarkdown == null) {
                loaded.notesByMarkdown = new LinkedHashMap<>();
            }
            if (loaded.taskTitle != null && loaded.taskTitle.trim().isEmpty()) {
                loaded.taskTitle = null;
            }
            if (loaded.lastOpenedAt != null) {
                String normalizedLastOpened = loaded.lastOpenedAt.trim();
                if (normalizedLastOpened.isEmpty()) {
                    loaded.lastOpenedAt = null;
                } else {
                    Instant parsed = parseInstantSafe(normalizedLastOpened);
                    loaded.lastOpenedAt = parsed != null ? parsed.toString() : null;
                }
            }
            for (Map.Entry<String, NoteMeta> entry : loaded.notesByMarkdown.entrySet()) {
                if (entry.getValue() == null) {
                    entry.setValue(new NoteMeta());
                    continue;
                }
                if (entry.getValue().favorites == null) {
                    entry.getValue().favorites = new LinkedHashMap<>();
                }
                if (entry.getValue().deleted == null) {
                    entry.getValue().deleted = new LinkedHashMap<>();
                }
                if (entry.getValue().comments == null) {
                    entry.getValue().comments = new LinkedHashMap<>();
                } else {
                    entry.getValue().comments = new LinkedHashMap<>(sanitizeComments(entry.getValue().comments));
                }
                if (entry.getValue().tokenLike == null) {
                    entry.getValue().tokenLike = new LinkedHashMap<>();
                } else {
                    entry.getValue().tokenLike = sanitizeTokenLike(entry.getValue().tokenLike);
                }
                if (entry.getValue().tokenAnnotations == null) {
                    entry.getValue().tokenAnnotations = new LinkedHashMap<>();
                } else {
                    entry.getValue().tokenAnnotations = new LinkedHashMap<>(sanitizeTokenAnnotations(entry.getValue().tokenAnnotations));
                }
            }
            return loaded;
        } catch (Exception ex) {
            logger.warn("read task metadata failed: {} err={}", metaPath, ex.getMessage());
            return fallback;
        }
    }

    private boolean writeTaskMeta(Path taskRoot, TaskMetaFile meta) {
        if (taskRoot == null || meta == null) {
            return false;
        }
        Path metaPath = taskRoot.resolve(META_FILE_NAME).normalize();
        Path tmpPath = taskRoot.resolve(META_FILE_NAME + ".tmp").normalize();
        if (!metaPath.startsWith(taskRoot)) {
            return false;
        }
        try {
            Files.createDirectories(taskRoot);
            meta.version = "1.0";
            meta.updatedAt = Instant.now().toString();
            if (meta.notesByMarkdown == null) {
                meta.notesByMarkdown = new LinkedHashMap<>();
            }
            // Write to temp file first, then replace target file to avoid truncation on serialization failures.
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmpPath.toFile(), meta);
            try {
                Files.move(tmpPath, metaPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(tmpPath, metaPath, StandardCopyOption.REPLACE_EXISTING);
            }
            return true;
        } catch (Exception ex) {
            try {
                Files.deleteIfExists(tmpPath);
            } catch (Exception ignored) {
                // Temp file cleanup failure should not affect main flow.
            }
                logger.warn("write task metadata failed: {} err={}", metaPath, ex.getMessage());
            return false;
        }
    }

    private boolean syncVideoMetaTitle(Path taskRoot, String title) {
        if (taskRoot == null) {
            return false;
        }
        Path videoMetaPath = taskRoot.resolve("video_meta.json").normalize();
        Path tmpPath = taskRoot.resolve("video_meta.json.tmp").normalize();
        if (!videoMetaPath.startsWith(taskRoot)) {
            return false;
        }
        try {
            Files.createDirectories(taskRoot);
            ObjectNode root = loadVideoMetaNode(videoMetaPath);
            if (title == null || title.isBlank()) {
                root.remove("title");
            } else {
                root.put("title", title);
            }
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmpPath.toFile(), root);
            try {
                Files.move(tmpPath, videoMetaPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(tmpPath, videoMetaPath, StandardCopyOption.REPLACE_EXISTING);
            }
            return true;
        } catch (Exception ex) {
            try {
                Files.deleteIfExists(tmpPath);
            } catch (Exception ignored) {
                // Ignore tmp cleanup failures.
            }
            logger.warn("write video metadata title failed: {} err={}", videoMetaPath, ex.getMessage());
            return false;
        }
    }

    private ObjectNode loadVideoMetaNode(Path videoMetaPath) {
        if (videoMetaPath == null || !Files.isRegularFile(videoMetaPath)) {
            return objectMapper.createObjectNode();
        }
        try {
            if (Files.size(videoMetaPath) == 0L) {
                return objectMapper.createObjectNode();
            }
            JsonNode loaded = objectMapper.readTree(videoMetaPath.toFile());
            if (loaded instanceof ObjectNode) {
                return (ObjectNode) loaded;
            }
            return objectMapper.createObjectNode();
        } catch (Exception ex) {
            logger.warn("read video metadata failed: {} err={}", videoMetaPath, ex.getMessage());
            return objectMapper.createObjectNode();
        }
    }

    private String normalizeMetaNoteKey(Path taskRoot, String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return META_DEFAULT_NOTE_KEY;
        }
        String decoded = URLDecoder.decode(rawPath, StandardCharsets.UTF_8).trim();
        if (decoded.isBlank()) {
            return META_DEFAULT_NOTE_KEY;
        }
        try {
            Path input = Paths.get(decoded);
            if (input.isAbsolute()) {
                Path normalizedAbs = input.toAbsolutePath().normalize();
                if (taskRoot != null) {
                    Path normalizedRoot = taskRoot.toAbsolutePath().normalize();
                    if (normalizedAbs.startsWith(normalizedRoot)) {
                        Path relative = normalizedRoot.relativize(normalizedAbs);
                        String key = relative.toString().replace('\\', '/');
                        return key.isBlank() ? META_DEFAULT_NOTE_KEY : key;
                    }
                }
            }
        } catch (Exception ignored) {
            // Treat illegal path as plain string fallback.
        }
        while (decoded.startsWith("/") || decoded.startsWith("\\")) {
            decoded = decoded.substring(1);
        }
        decoded = decoded.replace('\\', '/');
        return decoded.isBlank() ? META_DEFAULT_NOTE_KEY : decoded;
    }

    private Map<String, Boolean> sanitizeFavorites(Map<String, Boolean> input) {
        return sanitizeBooleanFlags(input);
    }

    private Map<String, Boolean> sanitizeDeleted(Map<String, Boolean> input) {
        return sanitizeBooleanFlags(input);
    }

    private Map<String, Boolean> sanitizeTokenLike(Map<String, Boolean> input) {
        return sanitizeBooleanFlags(input);
    }

    private Map<String, Boolean> sanitizeBooleanFlags(Map<String, Boolean> input) {
        Map<String, Boolean> output = new LinkedHashMap<>();
        if (input == null) {
            return output;
        }
        for (Map.Entry<String, Boolean> entry : input.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) continue;
            if (Boolean.TRUE.equals(entry.getValue())) {
                output.put(entry.getKey(), true);
            }
        }
        return output;
    }

    private Map<String, List<String>> sanitizeComments(Map<String, ?> input) {
        Map<String, List<String>> output = new LinkedHashMap<>();
        for (Map.Entry<String, ?> entry : input.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) continue;
            Object raw = entry.getValue();
            if (raw == null) continue;

            List<String> normalized = new ArrayList<>();
            if (raw instanceof String) {
                String trimmed = ((String) raw).trim();
                if (!trimmed.isEmpty()) {
                    normalized.add(trimmed);
                }
            } else if (raw instanceof List<?> list) {
                for (Object item : list) {
                    if (item == null) continue;
                    String trimmed = String.valueOf(item).trim();
                    if (!trimmed.isEmpty()) {
                        normalized.add(trimmed);
                    }
                }
            } else {
                String trimmed = String.valueOf(raw).trim();
                if (!trimmed.isEmpty()) {
                    normalized.add(trimmed);
                }
            }

            if (!normalized.isEmpty()) {
                output.put(entry.getKey(), normalized);
            }
        }
        return output;
    }

    private Map<String, String> sanitizeTokenAnnotations(Map<String, ?> input) {
        Map<String, String> output = new LinkedHashMap<>();
        if (input == null) {
            return output;
        }
        for (Map.Entry<String, ?> entry : input.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) continue;
            if (entry.getValue() == null) continue;
            String value = String.valueOf(entry.getValue()).trim();
            if (!value.isEmpty()) {
                output.put(entry.getKey().trim(), value);
            }
        }
        return output;
    }

    private Map<String, String> sanitizeTelemetryPayload(Map<String, ?> input) {
        Map<String, String> output = new LinkedHashMap<>();
        if (input == null) {
            return output;
        }
        for (Map.Entry<String, ?> entry : input.entrySet()) {
            if (entry.getKey() == null || entry.getKey().isBlank()) continue;
            if (entry.getValue() == null) continue;
            String value = String.valueOf(entry.getValue()).trim();
            if (!value.isEmpty()) {
                output.put(entry.getKey(), value);
            }
        }
        return output;
    }

    private String trimToNullSafe(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private List<ExportFileEntry> collectExportEntries(Path taskRoot) throws IOException {
        Path normalizedRoot = taskRoot.toAbsolutePath().normalize();
        List<ExportFileEntry> entries = new ArrayList<>();
        Set<String> addedEntries = new HashSet<>();

        Files.walkFileTree(normalizedRoot, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                if (normalizedRoot.equals(dir)) {
                    return FileVisitResult.CONTINUE;
                }
                if (shouldExcludeExportPath(normalizedRoot, dir)) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!attrs.isRegularFile()) {
                    return FileVisitResult.CONTINUE;
                }
                if (shouldExcludeExportPath(normalizedRoot, file)) {
                    return FileVisitResult.CONTINUE;
                }
                Path normalizedFile = file.toAbsolutePath().normalize();
                String entryName = normalizedRoot.relativize(normalizedFile).toString().replace('\\', '/');
                if (entryName.isBlank() || !addedEntries.add(entryName)) {
                    return FileVisitResult.CONTINUE;
                }
                entries.add(new ExportFileEntry(normalizedFile, entryName, isCoreExportFile(normalizedFile)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                logger.warn("walk export directory failed; skipped: root={} path={} err={}",
                        normalizedRoot, file, exc != null ? exc.getMessage() : "unknown");
                return FileVisitResult.CONTINUE;
            }
        });

        return entries;
    }

    private boolean shouldExcludeExportPath(Path taskRoot, Path candidate) {
        Path normalizedRoot = taskRoot.toAbsolutePath().normalize();
        Path normalizedCandidate = candidate.toAbsolutePath().normalize();
        if (!normalizedCandidate.startsWith(normalizedRoot)) {
            return true;
        }
        Path relative;
        try {
            relative = normalizedRoot.relativize(normalizedCandidate);
        } catch (Exception ex) {
            return true;
        }
        for (Path part : relative) {
            String segment = part.toString();
            if (!segment.isBlank() && segment.startsWith(".")) {
                return true;
            }
        }
        String filename = normalizedCandidate.getFileName() == null ? "" : normalizedCandidate.getFileName().toString();
        return isTemporaryExportName(filename);
    }

    private boolean isTemporaryExportName(String filename) {
        if (filename == null || filename.isBlank()) {
            return true;
        }
        String lower = filename.toLowerCase(Locale.ROOT);
        if (lower.equals(".ds_store") || lower.equals("thumbs.db") || lower.equals("desktop.ini")) {
            return true;
        }
        if (lower.startsWith("~$") || lower.endsWith("~")) {
            return true;
        }
        return lower.endsWith(".tmp")
                || lower.endsWith(".temp")
                || lower.endsWith(".swp")
                || lower.endsWith(".swo")
                || lower.endsWith(".part")
                || lower.endsWith(".partial")
                || lower.endsWith(".crdownload")
                || lower.endsWith(".download");
    }

    private boolean isCoreExportFile(Path path) {
        String filename = path.getFileName() == null ? "" : path.getFileName().toString();
        return isMarkdownFile(filename);
    }

    private ExportZipResult writeTaskExportZipStreaming(List<ExportFileEntry> entries, ZipOutputStream zos) throws IOException {
        int exportedCount = 0;
        int skippedCount = 0;
        for (ExportFileEntry entry : entries) {
            boolean opened = false;
            try {
                zos.putNextEntry(new ZipEntry(entry.entryName));
                opened = true;
                Files.copy(entry.path, zos);
                exportedCount += 1;
            } catch (IOException ex) {
                if (entry.core) {
                    throw new IOException("core file write to ZIP failed: " + entry.entryName, ex);
                }
                skippedCount += 1;
                logger.warn("skip file while exporting ZIP: entry={} path={} err={}",
                        entry.entryName, entry.path, ex.getMessage());
            } finally {
                if (opened) {
                    zos.closeEntry();
                }
            }
        }
        return new ExportZipResult(exportedCount, skippedCount);
    }

    private int markdownNamePriority(String filename) {
        if (filename == null) {
            return 100;
        }
        String lower = filename.toLowerCase(Locale.ROOT);
        if ("enhanced_output.md".equals(lower)) return 0;
        if ("enhanced_output2.md".equals(lower)) return 1;
        if ("output.md".equals(lower)) return 2;
        return 10;
    }

    private long safeLastModifiedMillis(Path path) {
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (Exception ex) {
            return 0L;
        }
    }

    private long bestTimestamp(TaskView task) {
        if (task != null && task.lastOpenedAt != null) {
            return task.lastOpenedAt.toEpochMilli();
        }
        if (task.createdAt != null) {
            return task.createdAt.toEpochMilli();
        }
        if (task.completedAt != null) {
            return task.completedAt.toEpochMilli();
        }
        return 0L;
    }

    private Path resolveStorageRoot() {
        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        for (int i = 0; i < 8; i++) {
            Path candidate = current.resolve("var").resolve("storage").resolve("storage");
            if (Files.isDirectory(candidate)) {
                return candidate.toAbsolutePath().normalize();
            }
            Path parent = current.getParent();
            if (parent == null) {
                break;
            }
            current = parent;
        }
        return Paths.get("var", "storage", "storage").toAbsolutePath().normalize();
    }

    private boolean isSafeStorageKey(String storageKey) {
        if (storageKey == null || storageKey.isBlank()) {
            return false;
        }
        return !storageKey.contains("..")
                && !storageKey.contains("/")
                && !storageKey.contains("\\");
    }

    private String deriveStorageTitle(Path taskDir, StorageMetadata metadata) {
        if (metadata.videoPath != null && !metadata.videoPath.isBlank()) {
            try {
                Path fileName = Paths.get(metadata.videoPath).getFileName();
                if (fileName != null && !fileName.toString().isBlank()) {
                    return fileName.toString();
                }
            } catch (Exception ignored) {
                // Fallback to directory name.
            }
        }
        return taskDir.getFileName().toString();
    }

    private void applyTaskTitleFromMeta(TaskView view) {
        if (view == null) {
            return;
        }
        Path root = view.taskRootDir;
        if (root == null) {
            return;
        }
        TaskMetaFile meta = readTaskMeta(root);
        if (meta.taskTitle != null && !meta.taskTitle.isBlank()) {
            String normalizedTitle = meta.taskTitle.trim();
            view.title = normalizedTitle;
            view.metaTitle = normalizedTitle;
        }
        if (meta.lastOpenedAt != null && !meta.lastOpenedAt.isBlank()) {
            view.lastOpenedAt = parseInstantSafe(meta.lastOpenedAt);
        }
    }

    private Instant markTaskOpened(TaskView task) {
        if (task == null) {
            return null;
        }
        Path taskRoot;
        try {
            taskRoot = resolveTaskRootDir(task);
        } catch (Exception ex) {
            return null;
        }

        TaskMetaFile meta = readTaskMeta(taskRoot);
        String normalizedTitle = trimToNullSafe(task.title);
        String normalizedTaskId = trimToNullSafe(task.taskId);
        if ((meta.taskTitle == null || meta.taskTitle.isBlank())
                && normalizedTitle != null
                && (normalizedTaskId == null || !normalizedTitle.equals(normalizedTaskId))) {
            meta.taskTitle = normalizedTitle;
            task.title = normalizedTitle;
            task.metaTitle = normalizedTitle;
        }

        Instant openedAt = Instant.now();
        meta.lastOpenedAt = openedAt.toString();
        if (!writeTaskMeta(taskRoot, meta)) {
            return null;
        }
        task.lastOpenedAt = openedAt;
        return openedAt;
    }

    private Instant parseInstantSafe(String rawValue) {
        String normalized = trimToNullSafe(rawValue);
        if (normalized == null) {
            return null;
        }
        try {
            return Instant.parse(normalized);
        } catch (Exception ex) {
            return null;
        }
    }

    private String deriveTaskTitle(String videoUrl, String fallbackTaskId) {
        return TaskDisplayNameResolver.resolveTaskDisplayTitle(videoUrl, fallbackTaskId);
    }

    private String instantToText(Instant instant) {
        return instant == null ? "" : instant.toString();
    }




    private String normalizeVideoInput(String rawVideoInput) {
        return VideoInputNormalizer.normalizeVideoInput(rawVideoInput);
    }

    private TaskQueueManager.Priority resolvePriority(String normalizedUserId, String rawPriority) {
        if (StringUtils.hasText(rawPriority)) {
            logger.info("ignore client priority parameter: user={} priority={}", normalizedUserId, rawPriority);
        }
        return TaskQueueManager.Priority.NORMAL;
    }

    private String normalizeUserId(String rawUserId) {
        String trimmed = rawUserId != null ? rawUserId.trim() : "";
        if (!trimmed.isEmpty()) {
            return trimmed;
        }
        return "mobile_user_" + System.currentTimeMillis();
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
        String uniquePrefix = Instant.now().toEpochMilli() + "_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        Path targetPath = uploadRootPath.resolve(uniquePrefix + "_" + safeFileName).toAbsolutePath().normalize();
        if (!targetPath.startsWith(uploadRootPath)) {
            throw new IOException("illegal upload path");
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

    private boolean isMarkdownFile(String filename) {
        if (filename == null) {
            return false;
        }
        String lower = filename.toLowerCase(Locale.ROOT);
        return lower.endsWith(".md") || lower.endsWith(".markdown");
    }

    private MediaType detectMediaType(Path path) {
        try {
            String probed = Files.probeContentType(path);
            if (probed != null && !probed.isBlank()) {
                return MediaType.parseMediaType(probed);
            }
        } catch (Exception ignored) {
            // Keep extension fallback checks.
        }
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        if (name.endsWith(".md") || name.endsWith(".markdown")) {
            return MediaType.parseMediaType("text/markdown;charset=UTF-8");
        }
        if (name.endsWith(".png")) return MediaType.IMAGE_PNG;
        if (name.endsWith(".jpg") || name.endsWith(".jpeg")) return MediaType.IMAGE_JPEG;
        if (name.endsWith(".gif")) return MediaType.IMAGE_GIF;
        if (name.endsWith(".webp")) return MediaType.parseMediaType("image/webp");
        if (name.endsWith(".svg")) return MediaType.parseMediaType("image/svg+xml");
        if (name.endsWith(".mp4")) return MediaType.parseMediaType("video/mp4");
        if (name.endsWith(".webm")) return MediaType.parseMediaType("video/webm");
        if (name.endsWith(".mov")) return MediaType.parseMediaType("video/quicktime");
        return MediaType.APPLICATION_OCTET_STREAM;
    }

    private static class ExportFileEntry {
        private final Path path;
        private final String entryName;
        private final boolean core;

        private ExportFileEntry(Path path, String entryName, boolean core) {
            this.path = path;
            this.entryName = entryName;
            this.core = core;
        }
    }

    private static class ExportZipResult {
        private final int exportedCount;
        private final int skippedCount;

        private ExportZipResult(int exportedCount, int skippedCount) {
            this.exportedCount = exportedCount;
            this.skippedCount = skippedCount;
        }
    }

    private static class ResolvedMarkdown {
        private final Path markdownPath;
        private final Path baseDir;

        private ResolvedMarkdown(Path markdownPath, Path baseDir) {
            this.markdownPath = markdownPath;
            this.baseDir = baseDir;
        }
    }

    private static class StorageMetadata {
        private boolean hasSuccessFlag;
        private boolean success;
        private Instant generatedAt;
        private String errorMessage;
        private String videoPath;
        private String resultMarkdownPath;
    }

    private static class TaskMetaFile {
        public String version = "1.0";
        public String updatedAt = "";
        public String taskTitle = null;
        public String lastOpenedAt = null;
        public Map<String, NoteMeta> notesByMarkdown = new LinkedHashMap<>();
    }

    private static class NoteMeta {
        public Map<String, Boolean> favorites = new LinkedHashMap<>();
        public Map<String, Boolean> deleted = new LinkedHashMap<>();
        public Map<String, Object> comments = new LinkedHashMap<>();
        public Map<String, Boolean> tokenLike = new LinkedHashMap<>();
        public Map<String, Object> tokenAnnotations = new LinkedHashMap<>();
    }

    public static class MarkdownUpdateRequest {
        public String path;
        public String markdown;
    }

    public static class TaskMetaUpdateRequest {
        public String path;
        public String taskTitle;
        public Map<String, Boolean> favorites;
        public Map<String, Boolean> deleted;
        public Map<String, Object> comments;
        public Map<String, Boolean> tokenLike;
        public Map<String, Object> tokenAnnotations;
    }

    public static class TaskTelemetryIngestRequest {
        public String path;
        public List<TelemetryEventItem> events;
    }

    public static class TelemetryEventItem {
        public String nodeId;
        public String eventType;
        public Double relevanceScore;
        public Long timestampMs;
        public Map<String, Object> payload;
    }

    public static class TaskSubmitRequest {
        public String userId;
        public String videoUrl;
        public String outputDir;
        public String priority;
        public String collectionId;
        public Integer episodeNo;
    }

    public static class CollectionBatchSubmitRequest {
        public List<Integer> episodeNos;
        public String userId;
        public String outputDir;
        public String priority;
    }

    private static class TaskView {
        private String taskId;
        private String title;
        private String metaTitle;
        private String videoUrl;
        private String status;
        private Instant createdAt;
        private Instant lastOpenedAt;
        private Instant completedAt;
        private String resultPath;
        private boolean markdownAvailable;
        private Path markdownPath;
        private Path baseDir;
        private boolean storageTask;
        private String storageKey;
        private Path taskRootDir;
        private String statusMessage;
        private double progress;
        private TaskEntry runtimeTask;
        private String collectionId;
        private Integer episodeNo;
        private String episodeTitle;
        private String collectionTitle;
        private Integer totalEpisodes;
    }
}
