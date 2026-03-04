package com.mvp.module2.fusion.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mvp.module2.fusion.common.TaskDisplayNameResolver;
import com.mvp.module2.fusion.common.UserFacingErrorMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskStatus;
import com.mvp.module2.fusion.service.CollectionRepository;
import com.mvp.module2.fusion.service.FileTransferService;
import com.mvp.module2.fusion.service.FileReuseService;
import com.mvp.module2.fusion.service.VideoMetaService;
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
import org.springframework.web.multipart.MultipartException;
import org.springframework.web.server.ResponseStatusException;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import java.util.regex.Matcher;
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
    private static final Pattern MARKDOWN_LINK_PATTERN = Pattern.compile("(!?\\[[^\\]]*])\\(([^)\\s]+)([^)]*)\\)");
    private static final Pattern BOOK_SELECTOR_PATTERN = Pattern.compile("c(\\d+)s(\\d+)(?:t(\\d+))?", Pattern.CASE_INSENSITIVE);
    private static final Set<String> ALLOWED_UPLOAD_EXTENSIONS = Set.of(
            ".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v",
            ".txt", ".md", ".pdf", ".epub"
    );
    private static final long MAX_UPLOAD_FILE_BYTES = 2L * 1024L * 1024L * 1024L;
    private static final Pattern SAFE_UPLOAD_ID_PATTERN = Pattern.compile("^[A-Za-z0-9_-]{8,96}$");

    @Autowired
    private TaskQueueManager taskQueueManager;

    @Autowired(required = false)
    private PythonGrpcClient pythonGrpcClient;

    @Autowired
    private com.mvp.module2.fusion.service.StorageTaskCacheService storageTaskCacheService;

    @Autowired(required = false)
    private com.mvp.module2.fusion.service.PersonaAwareReadingService personaAwareReadingService;

    @Autowired(required = false)
    private com.mvp.module2.fusion.service.PersonaInsightCardService personaInsightCardService;

    @Autowired(required = false)
    private CollectionRepository collectionRepository;

    @Autowired(required = false)
    private FileReuseService fileReuseService;

    @Autowired
    private FileTransferService fileTransferService;

    private VideoMetaService videoMetaService = new VideoMetaService();

    @Value("${task.upload.dir:var/uploads}")
    private String uploadDir;

    @Value("${mobile.video-info.timeout-seconds:30}")
    private int mobileVideoInfoTimeoutSeconds;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired(required = false)
    public void setVideoMetaService(VideoMetaService videoMetaService) {
        if (videoMetaService != null) {
            this.videoMetaService = videoMetaService;
        }
    }

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
        finalViewList = deduplicateTaskViews(finalViewList);
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
        TaskQueueManager.BookProcessingOptions bookOptions = buildBookProcessingOptions(
                request.chapterSelector,
                request.sectionSelector,
                request.splitByChapter,
                request.splitBySection,
                request.pageOffset
        );
        String lockedTitle = resolveSubmissionTaskTitle(request.videoUrl, normalizedVideoInput);
        logger.info("Mobile task submission: raw={} normalized={} user={}", request.videoUrl, normalizedVideoInput, normalizedUserId);
        TaskQueueManager.TaskEntry task = taskQueueManager.submitTask(
                normalizedUserId,
                normalizedVideoInput,
                normalizeOutputDir(request.outputDir),
                priority,
                lockedTitle,
                bookOptions
        );
        linkCollectionEpisodeIfNecessary(request.collectionId, request.episodeNo, task.taskId);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("taskId", task.taskId);
        payload.put("title", task.title != null ? task.title : "");
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
    public CompletableFuture<ResponseEntity<Map<String, Object>>> submitUploadTaskFromMobile(
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
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "videoFile cannot be empty"
            )));
        }
        if (videoFile.getSize() > MAX_UPLOAD_FILE_BYTES) {
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "uploaded file is too large; current limit is 2048MB"
            )));
        }

        String safeFileName = sanitizeUploadFileName(videoFile.getOriginalFilename());
        if (!hasSupportedUploadExtension(safeFileName)) {
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "supported formats: mp4/mov/mkv/avi/webm/m4v/txt/md/pdf/epub"
            )));
        }

        String normalizedUserId = normalizeUserId(userId);
        TaskQueueManager.Priority taskPriority = resolvePriority(normalizedUserId, priority);
        Optional<FileReuseService.FileFingerprint> fingerprintOpt =
                resolveFileFingerprint(fileMd5, fileExt, safeFileName);
        Optional<Path> reusedPathOpt = findReusableUploadPath(fingerprintOpt);
        if (reusedPathOpt.isPresent()) {
            return CompletableFuture.completedFuture(buildUploadSubmissionResponse(
                    reusedPathOpt.get(),
                    safeFileName,
                    normalizedUserId,
                    outputDir,
                    taskPriority,
                    chapterSelector,
                    sectionSelector,
                    splitByChapter,
                    splitBySection,
                    pageOffset,
                    probeOnly,
                    fingerprintOpt,
                    true
            ));
        }

        return fileTransferService
                .persistMultipartWithUniqueNameAsync(uploadDir, safeFileName, videoFile)
                .thenApply(savedVideoPath -> {
                    recordUploadedFileMetadata(savedVideoPath, safeFileName, videoFile.getSize(), fingerprintOpt);
                    return buildUploadSubmissionResponse(
                            savedVideoPath,
                            safeFileName,
                            normalizedUserId,
                            outputDir,
                            taskPriority,
                            chapterSelector,
                            sectionSelector,
                            splitByChapter,
                            splitBySection,
                            pageOffset,
                            probeOnly,
                            fingerprintOpt,
                            false
                    );
                })
                .exceptionally(error -> {
                    Throwable root = unwrapCompletionError(error);
                    logger.error("mobile upload video persistence failed", root);
                    return ResponseEntity.status(503).body(Map.of(
                            "success", false,
                            "message", UserFacingErrorMapper.busyMessage()
                    ));
                });
    }

    @PostMapping(value = "/tasks/upload/reuse-check", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> checkUploadFileReuse(
            @RequestBody(required = false) UploadReuseCheckRequest request
    ) {
        if (request == null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "request body cannot be empty"
            ));
        }
        String safeFileName = sanitizeUploadFileName(request.fileName);
        Optional<FileReuseService.FileFingerprint> fingerprintOpt =
                resolveFileFingerprint(request.fileMd5, request.fileExt, safeFileName);
        if (fingerprintOpt.isEmpty()) {
            return ResponseEntity.ok(Map.of(
                    "success", true,
                    "reused", false,
                    "message", "fingerprint missing or invalid"
            ));
        }
        Optional<Path> reusedPathOpt = findReusableUploadPath(fingerprintOpt);
        if (reusedPathOpt.isEmpty()) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("reused", false);
            payload.put("fileMd5", fingerprintOpt.get().md5());
            payload.put("fileExt", fingerprintOpt.get().fileExt());
            payload.put("message", "reuse candidate not found");
            return ResponseEntity.ok(payload);
        }

        boolean autoSubmit = !Boolean.FALSE.equals(request.autoSubmit);
        if (!autoSubmit) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("reused", true);
            payload.put("normalizedVideoUrl", reusedPathOpt.get().toString());
            payload.put("uploadedFileName", safeFileName);
            payload.put("fileMd5", fingerprintOpt.get().md5());
            payload.put("fileExt", fingerprintOpt.get().fileExt());
            payload.put("message", "reusable file located");
            return ResponseEntity.ok(payload);
        }

        String normalizedUserId = normalizeUserId(request.userId);
        TaskQueueManager.Priority taskPriority = resolvePriority(normalizedUserId, request.priority);
        return buildUploadSubmissionResponse(
                reusedPathOpt.get(),
                safeFileName,
                normalizedUserId,
                request.outputDir,
                taskPriority,
                request.chapterSelector,
                request.sectionSelector,
                request.splitByChapter,
                request.splitBySection,
                request.pageOffset,
                request.probeOnly,
                fingerprintOpt,
                true
        );
    }

    @PostMapping(value = "/tasks/upload/chunk", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public CompletableFuture<ResponseEntity<Map<String, Object>>> uploadTaskFileChunk(
            @RequestParam("uploadId") String uploadId,
            @RequestParam("chunkIndex") Integer chunkIndex,
            @RequestParam("totalChunks") Integer totalChunks,
            @RequestParam(value = "totalFileSize", required = false) Long totalFileSize,
            @RequestParam("fileName") String fileName,
            @RequestParam(value = "chunkSha256", required = false) String chunkSha256,
            @RequestParam("chunkFile") MultipartFile chunkFile
    ) {
        if (chunkFile == null || chunkFile.isEmpty()) {
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "chunkFile cannot be empty"
            )));
        }
        int normalizedChunkIndex = chunkIndex == null ? -1 : chunkIndex;
        int normalizedTotalChunks = totalChunks == null ? -1 : totalChunks;
        if (normalizedChunkIndex < 0 || normalizedTotalChunks <= 0 || normalizedChunkIndex >= normalizedTotalChunks) {
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "invalid chunk index or totalChunks"
            )));
        }
        String normalizedUploadId = normalizeUploadId(uploadId);
        String safeFileName = sanitizeUploadFileName(fileName);
        if (!hasSupportedUploadExtension(safeFileName)) {
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "supported formats: mp4/mov/mkv/avi/webm/m4v/txt/md/pdf/epub"
            )));
        }
        long normalizedTotalFileSize = totalFileSize == null ? -1L : totalFileSize;
        if (normalizedTotalFileSize > MAX_UPLOAD_FILE_BYTES) {
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "uploaded file is too large; current limit is 2048MB"
            )));
        }

        try {
            Path uploadRootPath = resolveUploadRoot();
            return fileTransferService
                    .writeChunkAsync(
                            uploadRootPath,
                            normalizedUploadId,
                            safeFileName,
                            normalizedTotalChunks,
                            normalizedTotalFileSize,
                            normalizedChunkIndex,
                            chunkFile,
                            chunkSha256
                    )
                    .thenApply(result -> {
                        Map<String, Object> payload = new LinkedHashMap<>();
                        payload.put("success", true);
                        payload.put("uploadId", normalizedUploadId);
                        payload.put("chunkIndex", normalizedChunkIndex);
                        payload.put("uploadedChunks", result.uploadedChunks.size());
                        payload.put("totalChunks", normalizedTotalChunks);
                        payload.put("chunkSha256", result.chunkSha256);
                        payload.put("chunkSizeBytes", result.chunkSizeBytes);
                        payload.put("message", "chunk uploaded");
                        return ResponseEntity.ok(payload);
                    })
                    .exceptionally(error -> mapChunkTransferError(
                            error,
                            normalizedUploadId,
                            normalizedChunkIndex,
                            "chunk upload persistence failed"
                    ));
        } catch (IOException e) {
            logger.error("chunk upload persistence failed: uploadId={} chunkIndex={}", normalizedUploadId, normalizedChunkIndex, e);
            return CompletableFuture.completedFuture(ResponseEntity.status(503).body(Map.of(
                    "success", false,
                    "message", UserFacingErrorMapper.busyMessage()
            )));
        }
    }

    @GetMapping("/tasks/upload/chunk/status")
    public ResponseEntity<Map<String, Object>> queryUploadChunkStatus(
            @RequestParam("uploadId") String uploadId
    ) {
        String normalizedUploadId = normalizeUploadId(uploadId);
        try {
            Path uploadRootPath = resolveUploadRoot();
            FileTransferService.ChunkSessionStatus status = fileTransferService.readChunkStatus(uploadRootPath, normalizedUploadId);
            if (!status.exists) {
                return ResponseEntity.ok(Map.of(
                        "success", true,
                        "exists", false,
                        "uploadId", normalizedUploadId,
                        "uploadedChunks", List.of()
                ));
            }
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("exists", true);
            payload.put("uploadId", normalizedUploadId);
            payload.put("uploadedChunks", status.uploadedChunks);
            payload.put("uploadedCount", status.uploadedChunks.size());
            if (status.meta != null) {
                payload.put("totalChunks", status.meta.totalChunks);
                payload.put("safeFileName", status.meta.safeFileName);
                payload.put("totalFileSize", status.meta.totalFileSize);
            }
            return ResponseEntity.ok(payload);
        } catch (IOException e) {
            logger.warn("query chunk status failed: uploadId={} err={}", normalizedUploadId, e.getMessage());
            return ResponseEntity.status(503).body(Map.of(
                    "success", false,
                    "message", UserFacingErrorMapper.busyMessage()
            ));
        }
    }

    @PostMapping(value = "/tasks/upload/chunk/complete", consumes = MediaType.APPLICATION_JSON_VALUE)
    public CompletableFuture<ResponseEntity<?>> completeChunkUpload(
            @RequestBody ChunkUploadCompleteRequest request
    ) {
        if (request == null || !StringUtils.hasText(request.uploadId)) {
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "uploadId cannot be empty"
            )));
        }
        String normalizedUploadId = normalizeUploadId(request.uploadId);
        String normalizedUserId = normalizeUserId(request.userId);
        TaskQueueManager.Priority taskPriority = resolvePriority(normalizedUserId, request.priority);

        final Path uploadRootPath;
        try {
            uploadRootPath = resolveUploadRoot();
        } catch (IOException e) {
            logger.error("resolve upload root failed for chunk complete: uploadId={}", normalizedUploadId, e);
            return CompletableFuture.completedFuture(ResponseEntity.status(503).body(Map.of(
                    "success", false,
                    "message", UserFacingErrorMapper.busyMessage()
            )));
        }

        return fileTransferService
                .readChunkStatusAsync(uploadRootPath, normalizedUploadId)
                .thenCompose(status -> {
                    if (!status.exists) {
                        return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                                "success", false,
                                "message", "chunk session not found"
                        )));
                    }
                    if (status.meta == null || !StringUtils.hasText(status.meta.safeFileName) || status.meta.totalChunks <= 0) {
                        return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                                "success", false,
                                "message", "chunk session metadata missing"
                        )));
                    }

                    Optional<FileReuseService.FileFingerprint> fingerprintOpt =
                            resolveFileFingerprint(request.fileMd5, request.fileExt, status.meta.safeFileName);
                    Optional<Path> reusedPathOpt = findReusableUploadPath(fingerprintOpt);
                    if (reusedPathOpt.isPresent()) {
                        fileTransferService.cleanupChunkSessionQuietly(uploadRootPath, normalizedUploadId);
                        ResponseEntity<Map<String, Object>> response = buildUploadSubmissionResponse(
                                reusedPathOpt.get(),
                                status.meta.safeFileName,
                                normalizedUserId,
                                request.outputDir,
                                taskPriority,
                                request.chapterSelector,
                                request.sectionSelector,
                                request.splitByChapter,
                                request.splitBySection,
                                request.pageOffset,
                                request.probeOnly,
                                fingerprintOpt,
                                true
                        );
                        return CompletableFuture.completedFuture(response);
                    }

                    if (status.meta.totalFileSize > MAX_UPLOAD_FILE_BYTES) {
                        return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(Map.of(
                                "success", false,
                                "message", "uploaded file is too large; current limit is 2048MB"
                        )));
                    }

                    return fileTransferService
                            .mergeChunkSessionAsync(uploadRootPath, normalizedUploadId)
                            .thenApply(mergeResult -> {
                                recordUploadedFileMetadata(
                                        mergeResult.mergedPath,
                                        status.meta.safeFileName,
                                        status.meta.totalFileSize,
                                        fingerprintOpt
                                );
                                ResponseEntity<Map<String, Object>> response = buildUploadSubmissionResponse(
                                        mergeResult.mergedPath,
                                        status.meta.safeFileName,
                                        normalizedUserId,
                                        request.outputDir,
                                        taskPriority,
                                        request.chapterSelector,
                                        request.sectionSelector,
                                        request.splitByChapter,
                                        request.splitBySection,
                                        request.pageOffset,
                                        request.probeOnly,
                                        fingerprintOpt,
                                        false
                                );
                                fileTransferService.cleanupChunkSessionQuietly(uploadRootPath, normalizedUploadId);
                                return response;
                            });
                })
                .exceptionally(error -> {
                    Throwable root = unwrapCompletionError(error);
                    if (root instanceof IllegalArgumentException) {
                        Map<String, Object> payload = new LinkedHashMap<>();
                        payload.put("success", false);
                        payload.put("message", root.getMessage() != null ? root.getMessage() : "invalid chunk upload request");
                        return ResponseEntity.badRequest().body(payload);
                    }
                    logger.error("complete chunk upload failed: uploadId={}", normalizedUploadId, root);
                    Map<String, Object> payload = new LinkedHashMap<>();
                    payload.put("success", false);
                    payload.put("message", UserFacingErrorMapper.busyMessage());
                    return ResponseEntity.status(503).body(payload);
                })
                .thenApply(response -> (ResponseEntity<?>) response);
    }

    @GetMapping("/tasks/upload/probe-asset")
    public ResponseEntity<?> getUploadProbeAsset(
            @RequestParam("videoInput") String rawVideoInput,
            @RequestHeader(value = HttpHeaders.RANGE, required = false) String rangeHeader
    ) {
        String normalizedVideoInput = normalizeVideoInput(rawVideoInput);
        if (!StringUtils.hasText(normalizedVideoInput)) {
            return ResponseEntity.badRequest().body(Map.of("message", "videoInput cannot be empty"));
        }

        final Path uploadRootPath;
        try {
            uploadRootPath = resolveUploadRoot();
        } catch (IOException ex) {
            logger.warn("resolve upload root failed for probe asset: input={} err={}", normalizedVideoInput, ex.getMessage());
            return ResponseEntity.status(503).body(Map.of("message", UserFacingErrorMapper.busyMessage()));
        }

        String candidatePath = normalizedVideoInput;
        if (candidatePath.startsWith("file://")) {
            candidatePath = candidatePath.substring("file://".length());
            if (candidatePath.matches("^/[A-Za-z]:/.*")) {
                candidatePath = candidatePath.substring(1);
            }
        }

        final Path target;
        try {
            target = Paths.get(candidatePath).toAbsolutePath().normalize();
        } catch (Exception ex) {
            return ResponseEntity.badRequest().body(Map.of("message", "invalid probe file path"));
        }
        if (!target.startsWith(uploadRootPath)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("message", "probe file path is out of upload root"));
        }
        if (!Files.exists(target) || !Files.isRegularFile(target)) {
            return ResponseEntity.status(404).body(Map.of("message", "probe file not found"));
        }
        String lowerName = target.getFileName() != null ? target.getFileName().toString().toLowerCase(Locale.ROOT) : "";
        if (!lowerName.endsWith(".pdf")) {
            return ResponseEntity.badRequest().body(Map.of("message", "only pdf probe preview is supported"));
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

                            @Override
                            public int read() throws IOException {
                                if (remaining <= 0) return -1;
                                int b = super.read();
                                if (b >= 0) remaining--;
                                return b;
                            }

                            @Override
                            public int read(byte[] b, int off, int len) throws IOException {
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
                    logger.warn("probe asset range response fallback to full: path={} range={} err={}",
                            target, rangeHeader, ex.getMessage());
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
            logger.warn("read probe asset failed: path={} err={}", target, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "read probe asset failed"));
        }
    }

    @org.springframework.web.bind.annotation.ExceptionHandler(MultipartException.class)
    public ResponseEntity<Map<String, Object>> handleMultipartUploadException(MultipartException ex) {
        Throwable root = ex;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        if (root instanceof EOFException) {
            logger.warn("multipart upload interrupted by client connection: {}", root.toString());
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "上传连接中断，请重试（已支持断点续传）",
                    "errorCode", "UPLOAD_STREAM_INTERRUPTED"
            ));
        }
        logger.warn("multipart parse failed: {}", ex.getMessage());
        return ResponseEntity.badRequest().body(Map.of(
                "success", false,
                "message", "上传数据格式异常，请重试",
                "errorCode", "UPLOAD_MULTIPART_INVALID"
        ));
    }

    private ResponseEntity<Map<String, Object>> mapChunkTransferError(
            Throwable error,
            String uploadId,
            int chunkIndex,
            String logMessage
    ) {
        Throwable root = unwrapCompletionError(error);
        if (root instanceof IllegalArgumentException) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", root.getMessage() != null ? root.getMessage() : "invalid chunk upload request"
            ));
        }
        logger.error("{}: uploadId={} chunkIndex={}", logMessage, uploadId, chunkIndex, root);
        return ResponseEntity.status(503).body(Map.of(
                "success", false,
                "message", UserFacingErrorMapper.busyMessage()
        ));
    }

    private Throwable unwrapCompletionError(Throwable error) {
        Throwable current = error;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current != null ? current : new RuntimeException("unknown transfer error");
    }

    @GetMapping("/tasks/{taskId}/markdown")
    public ResponseEntity<?> getTaskMarkdown(
            @PathVariable String taskId,
            @RequestParam(value = "userId", required = false) String userId,
            @RequestParam(value = "includePersonalization", defaultValue = "true") boolean includePersonalization
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
            response.put("markdownBytes", markdown.getBytes(StandardCharsets.UTF_8).length);
            response.put("markdownPath", resolved.markdownPath.toString());
            response.put("baseDir", resolved.baseDir.toString());
            response.put("assetEndpointTemplate", "/api/mobile/tasks/" + task.taskId + "/asset?path={path}");
            TocMetadata tocMetadata = resolveTaskTocMetadata(task, resolved.markdownPath);
            response.put("contentType", tocMetadata.contentType);
            response.put("bookSectionTree", tocMetadata.bookSectionTree);
            appendOrWarmupPersonalizedReading(
                    response,
                    task.taskId,
                    resolveReaderUserId(task, userId),
                    resolved.markdownPath,
                    markdown,
                    includePersonalization
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
            @RequestParam(value = "includePersonalization", defaultValue = "true") boolean includePersonalization,
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
            response.put("markdownBytes", markdown.getBytes(StandardCharsets.UTF_8).length);
            response.put("markdownPath", target.toString());
            response.put("baseDir", resolved.baseDir.toString());
            response.put("assetEndpointTemplate", "/api/mobile/tasks/" + task.taskId + "/asset?path={path}");
            TocMetadata tocMetadata = resolveTaskTocMetadata(task, target);
            response.put("contentType", tocMetadata.contentType);
            response.put("bookSectionTree", tocMetadata.bookSectionTree);
            appendOrWarmupPersonalizedReading(
                    response,
                    task.taskId,
                    resolveReaderUserId(task, userId),
                    target,
                    markdown,
                    includePersonalization
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
        payload.put("anchors", noteMeta.anchors != null ? sanitizeAnchors(noteMeta.anchors) : Map.of());
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
        if (request != null && request.anchors != null) {
            noteMeta.anchors = new LinkedHashMap<>(sanitizeAnchors(request.anchors));
        }
        if ((noteMeta.favorites == null || noteMeta.favorites.isEmpty())
                && (noteMeta.deleted == null || noteMeta.deleted.isEmpty())
                && (noteMeta.comments == null || noteMeta.comments.isEmpty())
                && (noteMeta.tokenLike == null || noteMeta.tokenLike.isEmpty())
                && (noteMeta.tokenAnnotations == null || noteMeta.tokenAnnotations.isEmpty())
                && (noteMeta.anchors == null || noteMeta.anchors.isEmpty())) {
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
        payload.put("anchors", noteMeta.anchors != null ? sanitizeAnchors(noteMeta.anchors) : Map.of());
        payload.put("metaPath", taskRoot.resolve(META_FILE_NAME).toString());
        payload.put("updatedAt", Instant.now().toString());
        return ResponseEntity.ok(payload);
    }

    @PostMapping(value = "/tasks/{taskId}/anchors/{anchorId}/mount", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<?> mountAnchorResources(
            @PathVariable String taskId,
            @PathVariable String anchorId,
            @RequestParam(value = "path", required = false) String rawPath,
            @RequestParam(value = "files", required = false) MultipartFile[] files,
            @RequestParam(value = "relativePaths", required = false) List<String> relativePaths,
            @RequestParam(value = "mainNotePath", required = false) String rawMainNotePath
    ) {
        String normalizedAnchorId = trimToNullSafe(anchorId);
        if (normalizedAnchorId == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "anchorId cannot be empty"));
        }
        if (files == null || files.length == 0) {
            return ResponseEntity.badRequest().body(Map.of("message", "missing upload files"));
        }

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

        String safeAnchorDirName = sanitizeAnchorDirectoryName(normalizedAnchorId);
        String revisionId = buildAnchorRevisionId();
        String relativeRevisionDir = "thinking/anchor_" + safeAnchorDirName + "/rev_" + revisionId;
        Path revisionDir = taskRoot.resolve(relativeRevisionDir).normalize();
        if (!revisionDir.startsWith(taskRoot)) {
            return ResponseEntity.badRequest().body(Map.of("message", "illegal anchor mount path"));
        }

        List<String> savedRelativePaths = new ArrayList<>();
        List<Map<String, Object>> savedFilePayload = new ArrayList<>();
        List<FileTransferService.BatchTransferItem> transferBatch = new ArrayList<>();
        List<MultipartFile> transferSourceFiles = new ArrayList<>();
        long totalBytes = 0L;
        try {
            Files.createDirectories(revisionDir);
            for (int index = 0; index < files.length; index++) {
                MultipartFile file = files[index];
                if (file == null || file.isEmpty()) {
                    continue;
                }
                String requestedRelative = (relativePaths != null && index < relativePaths.size())
                        ? relativePaths.get(index)
                        : null;
                String normalizedRelative = normalizeMountedRelativePath(requestedRelative, file.getOriginalFilename());
                if (normalizedRelative == null) {
                    return ResponseEntity.badRequest().body(Map.of(
                            "message", "illegal relative path in uploaded files",
                            "fileIndex", index
                    ));
                }
                Path target = revisionDir.resolve(normalizedRelative).normalize();
                if (!target.startsWith(revisionDir)) {
                    return ResponseEntity.badRequest().body(Map.of(
                            "message", "illegal target path in uploaded files",
                            "fileIndex", index
                    ));
                }
                if (target.getParent() != null) {
                    Files.createDirectories(target.getParent());
                }
                transferBatch.add(new FileTransferService.BatchTransferItem(target, file));
                transferSourceFiles.add(file);
                savedRelativePaths.add(normalizedRelative);
            }
            List<Path> persistedPaths = fileTransferService.persistMultipartBatchAsync(taskRoot, transferBatch).join();
            for (int index = 0; index < persistedPaths.size() && index < savedRelativePaths.size(); index++) {
                Path target = persistedPaths.get(index);
                MultipartFile sourceFile = transferSourceFiles.get(index);
                String normalizedRelative = savedRelativePaths.get(index);
                long fileSize = Files.size(target);
                totalBytes += fileSize;
                Map<String, Object> oneFilePayload = new LinkedHashMap<>();
                oneFilePayload.put("path", relativeRevisionDir + "/" + normalizedRelative);
                oneFilePayload.put("name", sourceFile.getOriginalFilename() != null ? sourceFile.getOriginalFilename() : target.getFileName().toString());
                oneFilePayload.put("size", fileSize);
                oneFilePayload.put("contentType", sourceFile.getContentType() != null ? sourceFile.getContentType() : "");
                savedFilePayload.add(oneFilePayload);
            }
        } catch (Exception ex) {
            logger.warn("mount anchor files failed: taskId={} anchorId={} err={}", taskId, normalizedAnchorId, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "write anchor files failed"));
        }

        if (savedRelativePaths.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "all uploaded files are empty"));
        }

        String selectedNoteRelative = resolveMainNoteRelativePath(rawMainNotePath, savedRelativePaths);
        String notePath = selectedNoteRelative != null ? relativeRevisionDir + "/" + selectedNoteRelative : "";
        String noteKey = normalizeMetaNoteKey(taskRoot, rawPath);
        Instant now = Instant.now();

        TaskMetaFile meta = readTaskMeta(taskRoot);
        NoteMeta noteMeta = meta.notesByMarkdown.getOrDefault(noteKey, new NoteMeta());
        Map<String, Object> anchors = sanitizeAnchors(noteMeta.anchors);
        Map<String, Object> anchorRecord = sanitizeSingleAnchorData(anchors.get(normalizedAnchorId));
        List<Map<String, Object>> revisions = sanitizeAnchorRevisionList(anchorRecord.get("revisions"));

        Map<String, Object> revisionPayload = new LinkedHashMap<>();
        revisionPayload.put("revisionId", revisionId);
        revisionPayload.put("createdAt", now.toString());
        revisionPayload.put("relativeDir", relativeRevisionDir);
        if (!notePath.isBlank()) {
            revisionPayload.put("notePath", notePath);
        }
        revisionPayload.put("fileCount", savedFilePayload.size());
        revisionPayload.put("totalBytes", totalBytes);
        revisionPayload.put("files", savedFilePayload);
        revisions.add(revisionPayload);

        anchorRecord.put("status", !notePath.isBlank() ? "mounted" : "files_uploaded");
        if (!notePath.isBlank()) {
            anchorRecord.put("mountedPath", notePath);
        }
        anchorRecord.put("mountedRevisionId", revisionId);
        anchorRecord.put("updatedAt", now.toString());
        anchorRecord.put("revisions", revisions);
        anchors.put(normalizedAnchorId, anchorRecord);
        noteMeta.anchors = anchors;
        meta.notesByMarkdown.put(noteKey, noteMeta);

        if (!writeTaskMeta(taskRoot, meta)) {
            return ResponseEntity.status(500).body(Map.of("message", "write task metadata failed"));
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("taskId", task.taskId);
        payload.put("pathKey", noteKey);
        payload.put("anchorId", normalizedAnchorId);
        payload.put("anchor", anchorRecord);
        payload.put("revision", revisionPayload);
        payload.put("updatedAt", now.toString());
        return ResponseEntity.ok(payload);
    }

    @PostMapping(value = "/tasks/{taskId}/anchors/{anchorId}/sync", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> syncAnchorResources(
            @PathVariable String taskId,
            @PathVariable String anchorId,
            @RequestBody AnchorSyncRequest request
    ) {
        String normalizedAnchorId = trimToNullSafe(anchorId);
        if (normalizedAnchorId == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "anchorId cannot be empty"));
        }
        if (request == null || request.operations == null || request.operations.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "missing sync operations"));
        }

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

        String noteKey = normalizeMetaNoteKey(taskRoot, request.path);
        TaskMetaFile meta = readTaskMeta(taskRoot);
        NoteMeta noteMeta = meta.notesByMarkdown.getOrDefault(noteKey, new NoteMeta());
        Map<String, Object> anchors = sanitizeAnchors(noteMeta.anchors);
        Map<String, Object> anchorRecord = sanitizeSingleAnchorData(anchors.get(normalizedAnchorId));
        if (anchorRecord.isEmpty()) {
            return ResponseEntity.status(404).body(Map.of("message", "anchor not found"));
        }

        List<Map<String, Object>> revisions = sanitizeAnchorRevisionList(anchorRecord.get("revisions"));
        Map<String, Object> latestRevision = revisions.isEmpty() ? Map.of() : revisions.get(revisions.size() - 1);
        String revisionRelativeDir = stringValueOrNull(latestRevision.get("relativeDir"));
        if (revisionRelativeDir == null) {
            String mountedPath = stringValueOrNull(anchorRecord.get("mountedPath"));
            if (mountedPath != null) {
                Path mountedParent = Paths.get(mountedPath).normalize().getParent();
                if (mountedParent != null) {
                    revisionRelativeDir = normalizeMountedRelativePath(mountedParent.toString().replace('\\', '/'), null);
                }
            }
        }
        if (revisionRelativeDir == null) {
            return ResponseEntity.status(400).body(Map.of("message", "anchor has no mounted revision directory"));
        }
        String normalizedRevisionRelativeDir = normalizeMountedRelativePath(revisionRelativeDir, null);
        if (normalizedRevisionRelativeDir == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "illegal revision directory"));
        }
        Path revisionDir = taskRoot.resolve(normalizedRevisionRelativeDir).normalize();
        if (!revisionDir.startsWith(taskRoot)) {
            return ResponseEntity.badRequest().body(Map.of("message", "illegal revision directory"));
        }
        try {
            Files.createDirectories(revisionDir);
        } catch (IOException ex) {
            logger.warn("prepare anchor revision directory failed: taskId={} anchorId={} dir={} err={}",
                    taskId, normalizedAnchorId, revisionDir, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "prepare anchor revision directory failed"));
        }

        List<Map<String, Object>> touchedFiles = new ArrayList<>();
        int upsertCount = 0;
        int deleteCount = 0;
        long totalBytes = 0L;
        for (int index = 0; index < request.operations.size(); index++) {
            AnchorSyncOperation op = request.operations.get(index);
            if (op == null) {
                continue;
            }
            String opType = trimToNullSafe(op.op);
            String normalizedRelativePath = normalizeMountedRelativePath(op.relativePath, null);
            if (normalizedRelativePath == null) {
                return ResponseEntity.badRequest().body(Map.of(
                        "message", "illegal relative path in sync operations",
                        "opIndex", index
                ));
            }
            Path target = revisionDir.resolve(normalizedRelativePath).normalize();
            if (!target.startsWith(revisionDir)) {
                return ResponseEntity.badRequest().body(Map.of(
                        "message", "sync target path out of revision directory",
                        "opIndex", index
                ));
            }

            String lowerOp = opType == null ? "" : opType.toLowerCase(Locale.ROOT);
            boolean isDelete = "delete".equals(lowerOp) || "remove".equals(lowerOp);
            boolean isUpsert = "add".equals(lowerOp)
                    || "replace".equals(lowerOp)
                    || "upsert".equals(lowerOp)
                    || "create".equals(lowerOp)
                    || "new".equals(lowerOp)
                    || "update".equals(lowerOp);
            if (!isDelete && !isUpsert) {
                continue;
            }

            try {
                if (isDelete) {
                    boolean deleted = Files.deleteIfExists(target);
                    if (deleted) {
                        deleteCount += 1;
                    }
                    Map<String, Object> one = new LinkedHashMap<>();
                    one.put("path", normalizedRevisionRelativeDir + "/" + normalizedRelativePath);
                    one.put("name", target.getFileName().toString());
                    one.put("size", 0L);
                    one.put("contentType", "application/x-delete");
                    touchedFiles.add(one);
                    continue;
                }

                if (target.getParent() != null) {
                    Files.createDirectories(target.getParent());
                }
                String content = op.content != null ? op.content : "";
                Files.writeString(
                        target,
                        content,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE
                );
                long fileSize = Files.size(target);
                totalBytes += fileSize;
                upsertCount += 1;
                Map<String, Object> one = new LinkedHashMap<>();
                one.put("path", normalizedRevisionRelativeDir + "/" + normalizedRelativePath);
                one.put("name", target.getFileName().toString());
                one.put("size", fileSize);
                one.put("contentType", "text/markdown");
                touchedFiles.add(one);
            } catch (Exception ex) {
                logger.warn("apply anchor sync op failed: taskId={} anchorId={} opIndex={} op={} path={} err={}",
                        taskId, normalizedAnchorId, index, lowerOp, normalizedRelativePath, ex.getMessage());
                return ResponseEntity.status(500).body(Map.of(
                        "message", "apply sync operation failed",
                        "opIndex", index
                ));
            }
        }

        if (upsertCount == 0 && deleteCount == 0) {
            return ResponseEntity.badRequest().body(Map.of("message", "no valid sync operations"));
        }

        String mountedPath = stringValueOrNull(anchorRecord.get("mountedPath"));
        String selectedMainRelative = normalizeMountedRelativePath(request.mainNotePath, null);
        String resolvedMainRelative = null;
        if (selectedMainRelative != null) {
            Path selectedMainTarget = revisionDir.resolve(selectedMainRelative).normalize();
            if (!selectedMainTarget.startsWith(revisionDir)) {
                return ResponseEntity.badRequest().body(Map.of("message", "main note path out of revision directory"));
            }
            if (Files.isRegularFile(selectedMainTarget) && isMarkdownFile(selectedMainRelative)) {
                resolvedMainRelative = selectedMainRelative;
            }
        }
        if (resolvedMainRelative == null) {
            String normalizedMountedPath = normalizeMountedRelativePath(mountedPath, null);
            String revisionPrefix = normalizedRevisionRelativeDir + "/";
            if (normalizedMountedPath != null && normalizedMountedPath.startsWith(revisionPrefix)) {
                String currentMainRelative = normalizedMountedPath.substring(revisionPrefix.length());
                Path currentMainTarget = revisionDir.resolve(currentMainRelative).normalize();
                if (currentMainTarget.startsWith(revisionDir)
                        && Files.isRegularFile(currentMainTarget)
                        && isMarkdownFile(currentMainRelative)) {
                    resolvedMainRelative = currentMainRelative;
                }
            }
        }
        if (resolvedMainRelative == null) {
            resolvedMainRelative = findFirstMarkdownRelativePath(revisionDir);
        }
        if (resolvedMainRelative != null) {
            mountedPath = normalizedRevisionRelativeDir + "/" + resolvedMainRelative;
        } else {
            mountedPath = null;
        }

        Instant now = Instant.now();
        String syncRevisionId = buildAnchorRevisionId();
        Map<String, Object> revisionPayload = new LinkedHashMap<>();
        revisionPayload.put("revisionId", syncRevisionId);
        revisionPayload.put("createdAt", now.toString());
        revisionPayload.put("relativeDir", normalizedRevisionRelativeDir);
        if (mountedPath != null && !mountedPath.isBlank()) {
            revisionPayload.put("notePath", mountedPath);
        }
        revisionPayload.put("fileCount", touchedFiles.size());
        revisionPayload.put("totalBytes", totalBytes);
        revisionPayload.put("files", touchedFiles);
        revisions.add(revisionPayload);

        if (mountedPath != null && !mountedPath.isBlank()) {
            anchorRecord.put("mountedPath", mountedPath);
            anchorRecord.put("status", "mounted");
        } else {
            anchorRecord.put("status", "files_uploaded");
        }
        anchorRecord.put("mountedRevisionId", syncRevisionId);
        anchorRecord.put("updatedAt", now.toString());
        anchorRecord.put("revisions", revisions);
        anchors.put(normalizedAnchorId, anchorRecord);
        noteMeta.anchors = anchors;
        meta.notesByMarkdown.put(noteKey, noteMeta);

        if (!writeTaskMeta(taskRoot, meta)) {
            return ResponseEntity.status(500).body(Map.of("message", "write task metadata failed"));
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("taskId", task.taskId);
        payload.put("pathKey", noteKey);
        payload.put("anchorId", normalizedAnchorId);
        payload.put("anchor", anchorRecord);
        payload.put("revision", revisionPayload);
        payload.put("upsertCount", upsertCount);
        payload.put("deleteCount", deleteCount);
        payload.put("updatedAt", now.toString());
        return ResponseEntity.ok(payload);
    }

    @PostMapping(value = "/tasks/{taskId}/anchors/delete", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> deleteAnchorsPermanently(
            @PathVariable String taskId,
            @RequestBody AnchorBatchDeleteRequest request
    ) {
        List<String> requestedAnchorIds = normalizeAnchorIdList(request != null ? request.anchorIds : null);
        if (requestedAnchorIds.isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "anchorIds cannot be empty"));
        }

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

        String noteKey = normalizeMetaNoteKey(taskRoot, request != null ? request.path : null);
        TaskMetaFile meta = readTaskMeta(taskRoot);
        NoteMeta noteMeta = meta.notesByMarkdown.getOrDefault(noteKey, new NoteMeta());
        Map<String, Object> anchors = sanitizeAnchors(noteMeta.anchors);

        List<String> existingAnchorIds = requestedAnchorIds.stream()
                .filter(anchors::containsKey)
                .toList();
        if (existingAnchorIds.isEmpty()) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("taskId", task.taskId);
            payload.put("pathKey", noteKey);
            payload.put("deletedCount", 0);
            payload.put("deletedAnchorIds", List.of());
            payload.put("missingAnchorIds", requestedAnchorIds);
            payload.put("deletedFileEntries", 0);
            payload.put("updatedAt", Instant.now().toString());
            return ResponseEntity.ok(payload);
        }

        boolean removeFiles = request == null || request.removeFiles == null || request.removeFiles;
        int deletedFileEntries = 0;
        if (removeFiles) {
            Set<Path> deleteTargets = collectAnchorDeleteTargets(taskRoot, anchors, existingAnchorIds);
            List<String> failedDeleteTargets = new ArrayList<>();
            for (Path target : deleteTargets) {
                try {
                    deletedFileEntries += deletePathRecursively(target, taskRoot);
                } catch (IOException ex) {
                    failedDeleteTargets.add(taskRoot.toAbsolutePath().normalize().relativize(target.toAbsolutePath().normalize()).toString().replace('\\', '/'));
                    logger.warn("delete anchor storage failed: taskId={} path={} err={}", taskId, target, ex.getMessage());
                }
            }
            if (!failedDeleteTargets.isEmpty()) {
                return ResponseEntity.status(500).body(Map.of(
                        "message", "delete anchor storage failed",
                        "failedPaths", failedDeleteTargets
                ));
            }
        }

        existingAnchorIds.forEach(anchors::remove);
        noteMeta.anchors = anchors;
        if (isNoteMetaEmpty(noteMeta)) {
            meta.notesByMarkdown.remove(noteKey);
        } else {
            meta.notesByMarkdown.put(noteKey, noteMeta);
        }
        if (!writeTaskMeta(taskRoot, meta)) {
            return ResponseEntity.status(500).body(Map.of("message", "write task metadata failed"));
        }

        List<String> missingAnchorIds = requestedAnchorIds.stream()
                .filter(id -> !existingAnchorIds.contains(id))
                .toList();

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("taskId", task.taskId);
        payload.put("pathKey", noteKey);
        payload.put("deletedCount", existingAnchorIds.size());
        payload.put("deletedAnchorIds", existingAnchorIds);
        payload.put("missingAnchorIds", missingAnchorIds);
        payload.put("deletedFileEntries", deletedFileEntries);
        payload.put("updatedAt", Instant.now().toString());
        return ResponseEntity.ok(payload);
    }

    @GetMapping("/tasks/{taskId}/anchors/{anchorId}/mounted")
    public ResponseEntity<?> getMountedAnchorNote(
            @PathVariable String taskId,
            @PathVariable String anchorId,
            @RequestParam(value = "path", required = false) String rawPath,
            @RequestParam(value = "notePath", required = false) String rawNotePath
    ) {
        String normalizedAnchorId = trimToNullSafe(anchorId);
        if (normalizedAnchorId == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "anchorId cannot be empty"));
        }

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
        Map<String, Object> anchors = sanitizeAnchors(noteMeta.anchors);
        Map<String, Object> anchorRecord = sanitizeSingleAnchorData(anchors.get(normalizedAnchorId));
        if (anchorRecord.isEmpty()) {
            return ResponseEntity.status(404).body(Map.of("message", "anchor not found"));
        }
        List<Map<String, Object>> revisions = sanitizeAnchorRevisionList(anchorRecord.get("revisions"));
        Map<String, Object> latestRevision = revisions.isEmpty() ? Map.of() : revisions.get(revisions.size() - 1);

        String entryNotePath = stringValueOrNull(latestRevision.get("notePath"));
        if (entryNotePath == null) {
            entryNotePath = stringValueOrNull(anchorRecord.get("mountedPath"));
        }
        if (entryNotePath == null) {
            return ResponseEntity.status(404).body(Map.of("message", "mounted markdown not found"));
        }
        String revisionRelativeDir = stringValueOrNull(latestRevision.get("relativeDir"));
        if (revisionRelativeDir == null) {
            Path entryPath = Paths.get(entryNotePath).normalize();
            Path entryParent = entryPath.getParent();
            if (entryParent != null) {
                revisionRelativeDir = entryParent.toString().replace('\\', '/');
            }
        }
        String notePath = resolveMountedReadNotePath(rawNotePath, entryNotePath, revisionRelativeDir);
        if (notePath == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "illegal mounted note path"));
        }
        Path target = taskRoot.resolve(notePath).normalize();
        if (!target.startsWith(taskRoot) || !Files.isRegularFile(target)) {
            return ResponseEntity.status(404).body(Map.of("message", "mounted markdown file missing"));
        }
        if (revisionRelativeDir != null && !revisionRelativeDir.isBlank()) {
            String normalizedRevisionRelativeDir = normalizeMountedRelativePath(revisionRelativeDir, null);
            if (normalizedRevisionRelativeDir != null) {
                Path revisionDir = taskRoot.resolve(normalizedRevisionRelativeDir).normalize();
                if (revisionDir.startsWith(taskRoot) && !target.startsWith(revisionDir)) {
                    return ResponseEntity.badRequest().body(Map.of("message", "mounted note path out of revision"));
                }
            }
        }

        try {
            String rawMarkdown = Files.readString(target, StandardCharsets.UTF_8);
            String baseRelativeDir = "";
            if (target.getParent() != null) {
                baseRelativeDir = taskRoot.relativize(target.getParent()).toString().replace('\\', '/');
            }
            String renderedMarkdown = rewriteMountedMarkdownAssetLinks(rawMarkdown, task.taskId, baseRelativeDir);

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("taskId", task.taskId);
            payload.put("pathKey", noteKey);
            payload.put("anchorId", normalizedAnchorId);
            payload.put("anchor", anchorRecord);
            payload.put("latestRevision", latestRevision);
            payload.put("entryNotePath", entryNotePath);
            payload.put("notePath", notePath);
            payload.put("assetBasePath", baseRelativeDir != null ? baseRelativeDir : "");
            payload.put("markdown", renderedMarkdown);
            payload.put("rawMarkdown", rawMarkdown);
            payload.put("updatedAt", Instant.now().toString());
            return ResponseEntity.ok(payload);
        } catch (Exception ex) {
            logger.warn("read mounted anchor note failed: taskId={} anchorId={} path={} err={}",
                    taskId, normalizedAnchorId, notePath, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "read mounted markdown failed"));
        }
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
        item.put("domain", task.domain != null ? task.domain : "");
        item.put("mainTopic", task.mainTopic != null ? task.mainTopic : "");
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

    private void appendOrWarmupPersonalizedReading(
            Map<String, Object> response,
            String taskId,
            String userId,
            Path markdownPath,
            String markdown,
            boolean includePersonalization
    ) {
        if (response == null) {
            return;
        }
        if (includePersonalization) {
            appendPersonalizedReading(response, taskId, userId, markdownPath, markdown);
            response.put("personalizationIncluded", true);
            return;
        }
        response.put("personalizationIncluded", false);
        if (personaAwareReadingService == null) {
            response.put("personalizationWarmupStatus", "unavailable");
            return;
        }
        String markdownPathText = markdownPath != null ? markdownPath.toString() : "";
        if (!StringUtils.hasText(markdownPathText)) {
            response.put("personalizationWarmupStatus", "skipped_no_markdown_path");
            return;
        }
        try {
            personaAwareReadingService.precomputeAsync(taskId, userId, markdownPathText);
            response.put("personalizationWarmupStatus", "started");
        } catch (Exception ex) {
            logger.warn("start personalization warmup failed: taskId={} err={}", taskId, ex.getMessage());
            response.put("personalizationWarmupStatus", "failed_to_start");
        }
    }

    private TaskView fromRuntimeTask(TaskEntry task) {
        TaskView view = new TaskView();
        view.taskId = task.taskId;
        String lockedTitle = trimToNullSafe(task.title);
        view.title = lockedTitle != null ? lockedTitle : deriveTaskTitle(task.videoUrl, task.taskId);
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
        String cachedTaskId = trimToNullSafe(cached.taskId);
        if (cachedTaskId != null) {
            view.taskId = cachedTaskId;
        } else {
            view.taskId = STORAGE_TASK_PREFIX + cached.storageKey;
        }
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
                if (entry.getValue().anchors == null) {
                    entry.getValue().anchors = new LinkedHashMap<>();
                } else {
                    entry.getValue().anchors = new LinkedHashMap<>(sanitizeAnchors(entry.getValue().anchors));
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
            ObjectNode root = videoMetaService.readOrCreateNode(taskRoot);
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

    private boolean isNoteMetaEmpty(NoteMeta noteMeta) {
        if (noteMeta == null) {
            return true;
        }
        return (noteMeta.favorites == null || noteMeta.favorites.isEmpty())
                && (noteMeta.deleted == null || noteMeta.deleted.isEmpty())
                && (noteMeta.comments == null || noteMeta.comments.isEmpty())
                && (noteMeta.tokenLike == null || noteMeta.tokenLike.isEmpty())
                && (noteMeta.tokenAnnotations == null || noteMeta.tokenAnnotations.isEmpty())
                && (noteMeta.anchors == null || noteMeta.anchors.isEmpty());
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

    private Map<String, Object> sanitizeAnchors(Map<String, ?> input) {
        Map<String, Object> output = new LinkedHashMap<>();
        if (input == null) {
            return output;
        }
        for (Map.Entry<String, ?> entry : input.entrySet()) {
            String anchorId = trimToNullSafe(entry.getKey());
            if (anchorId == null) {
                continue;
            }
            Map<String, Object> sanitized = sanitizeSingleAnchorData(entry.getValue());
            if (!sanitized.isEmpty()) {
                output.put(anchorId, sanitized);
            }
        }
        return output;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> sanitizeSingleAnchorData(Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> rawMap)) {
            return new LinkedHashMap<>();
        }
        Map<String, Object> value = (Map<String, Object>) rawMap;
        Map<String, Object> output = new LinkedHashMap<>();

        String blockId = stringValueOrNull(value.get("blockId"));
        if (blockId != null) {
            output.put("blockId", blockId);
        }
        Integer startIndex = nonNegativeIntOrNull(
                value.containsKey("startIndex") ? value.get("startIndex") : value.get("start")
        );
        Integer endIndex = nonNegativeIntOrNull(
                value.containsKey("endIndex") ? value.get("endIndex") : value.get("end")
        );
        if (startIndex != null) {
            output.put("startIndex", startIndex);
        }
        if (endIndex != null) {
            output.put("endIndex", endIndex);
        }

        String quote = stringValueOrNull(value.get("quote"));
        if (quote == null) {
            quote = stringValueOrNull(value.get("token"));
        }
        if (quote != null) {
            output.put("quote", quote);
        }
        String contextQuote = stringValueOrNull(value.get("contextQuote"));
        if (contextQuote == null) {
            contextQuote = stringValueOrNull(value.get("quoteSnapshot"));
        }
        if (contextQuote != null) {
            output.put("contextQuote", contextQuote);
        }
        String anchorHint = stringValueOrNull(value.get("anchorHint"));
        if (anchorHint == null) {
            anchorHint = stringValueOrNull(value.get("hint"));
        }
        if (anchorHint != null) {
            output.put("anchorHint", anchorHint);
        }

        String mountedPath = stringValueOrNull(value.get("mountedPath"));
        if (mountedPath != null) {
            output.put("mountedPath", mountedPath);
        }
        String mountedRevisionId = stringValueOrNull(value.get("mountedRevisionId"));
        if (mountedRevisionId != null) {
            output.put("mountedRevisionId", mountedRevisionId);
        }
        String updatedAt = normalizeIsoInstant(value.get("updatedAt"));
        if (updatedAt != null) {
            output.put("updatedAt", updatedAt);
        }
        String mountedAt = normalizeIsoInstant(value.get("mountedAt"));
        if (mountedAt != null) {
            output.put("mountedAt", mountedAt);
        }

        List<Map<String, Object>> revisions = sanitizeAnchorRevisionList(value.get("revisions"));
        if (!revisions.isEmpty()) {
            output.put("revisions", revisions);
            Map<String, Object> latest = revisions.get(revisions.size() - 1);
            if (!output.containsKey("mountedPath")) {
                String latestNotePath = stringValueOrNull(latest.get("notePath"));
                if (latestNotePath != null) {
                    output.put("mountedPath", latestNotePath);
                }
            }
            if (!output.containsKey("mountedRevisionId")) {
                String latestRevisionId = stringValueOrNull(latest.get("revisionId"));
                if (latestRevisionId != null) {
                    output.put("mountedRevisionId", latestRevisionId);
                }
            }
        }

        boolean hasMountedPath = output.containsKey("mountedPath");
        String status = normalizeAnchorStatus(value.get("status"), hasMountedPath);
        output.put("status", status);
        return output;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> sanitizeAnchorRevisionList(Object rawValue) {
        List<Map<String, Object>> output = new ArrayList<>();
        if (!(rawValue instanceof List<?> rawList)) {
            return output;
        }
        for (Object item : rawList) {
            if (!(item instanceof Map<?, ?> rawMap)) {
                continue;
            }
            Map<String, Object> one = sanitizeAnchorRevisionEntry((Map<String, Object>) rawMap);
            if (!one.isEmpty()) {
                output.add(one);
            }
        }
        return output;
    }

    private Map<String, Object> sanitizeAnchorRevisionEntry(Map<String, Object> rawMap) {
        Map<String, Object> output = new LinkedHashMap<>();
        String revisionId = stringValueOrNull(rawMap.get("revisionId"));
        if (revisionId == null) {
            revisionId = stringValueOrNull(rawMap.get("revision"));
        }
        if (revisionId != null) {
            output.put("revisionId", revisionId);
        }
        String createdAt = normalizeIsoInstant(rawMap.get("createdAt"));
        if (createdAt != null) {
            output.put("createdAt", createdAt);
        }
        String relativeDir = stringValueOrNull(rawMap.get("relativeDir"));
        if (relativeDir != null) {
            output.put("relativeDir", relativeDir);
        }
        String notePath = stringValueOrNull(rawMap.get("notePath"));
        if (notePath != null) {
            output.put("notePath", notePath);
        }
        Integer fileCount = nonNegativeIntOrNull(rawMap.get("fileCount"));
        if (fileCount != null) {
            output.put("fileCount", fileCount);
        }
        Long totalBytes = nonNegativeLongOrNull(rawMap.get("totalBytes"));
        if (totalBytes != null) {
            output.put("totalBytes", totalBytes);
        }
        List<Map<String, Object>> files = sanitizeAnchorRevisionFiles(rawMap.get("files"));
        if (!files.isEmpty()) {
            output.put("files", files);
        }
        return output;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> sanitizeAnchorRevisionFiles(Object rawValue) {
        List<Map<String, Object>> output = new ArrayList<>();
        if (!(rawValue instanceof List<?> rawList)) {
            return output;
        }
        for (Object item : rawList) {
            if (!(item instanceof Map<?, ?> rawMap)) {
                continue;
            }
            Map<String, Object> source = (Map<String, Object>) rawMap;
            String path = stringValueOrNull(source.get("path"));
            if (path == null) {
                continue;
            }
            Map<String, Object> fileItem = new LinkedHashMap<>();
            fileItem.put("path", path);
            String name = stringValueOrNull(source.get("name"));
            if (name != null) {
                fileItem.put("name", name);
            }
            Long size = nonNegativeLongOrNull(source.get("size"));
            if (size != null) {
                fileItem.put("size", size);
            }
            String contentType = stringValueOrNull(source.get("contentType"));
            if (contentType != null) {
                fileItem.put("contentType", contentType);
            }
            output.add(fileItem);
        }
        return output;
    }

    private String normalizeAnchorStatus(Object rawStatus, boolean hasMountedPath) {
        String normalized = stringValueOrNull(rawStatus);
        if (normalized == null) {
            return hasMountedPath ? "mounted" : "pending";
        }
        String lower = normalized.toLowerCase(Locale.ROOT);
        if ("mounted".equals(lower) || "pending".equals(lower) || "files_uploaded".equals(lower)) {
            return lower;
        }
        return hasMountedPath ? "mounted" : "pending";
    }

    private String normalizeIsoInstant(Object rawValue) {
        String value = stringValueOrNull(rawValue);
        if (value == null) {
            return null;
        }
        Instant parsed = parseInstantSafe(value);
        return parsed != null ? parsed.toString() : null;
    }

    private Integer nonNegativeIntOrNull(Object rawValue) {
        if (rawValue == null) {
            return null;
        }
        try {
            Integer parsed;
            if (rawValue instanceof Number number) {
                parsed = number.intValue();
            } else {
                parsed = Integer.parseInt(String.valueOf(rawValue).trim());
            }
            return parsed >= 0 ? parsed : null;
        } catch (Exception ex) {
            return null;
        }
    }

    private Long nonNegativeLongOrNull(Object rawValue) {
        if (rawValue == null) {
            return null;
        }
        try {
            Long parsed;
            if (rawValue instanceof Number number) {
                parsed = number.longValue();
            } else {
                parsed = Long.parseLong(String.valueOf(rawValue).trim());
            }
            return parsed >= 0 ? parsed : null;
        } catch (Exception ex) {
            return null;
        }
    }

    private String stringValueOrNull(Object rawValue) {
        if (rawValue == null) {
            return null;
        }
        String value = String.valueOf(rawValue).trim();
        return value.isEmpty() ? null : value;
    }

    private String sanitizeAnchorDirectoryName(String anchorId) {
        String normalized = anchorId == null ? "" : anchorId.trim();
        if (normalized.isEmpty()) {
            return "unknown";
        }
        String replaced = UNSAFE_FILENAME_CHARS.matcher(normalized).replaceAll("_");
        if (replaced.isBlank()) {
            return "unknown";
        }
        return replaced.length() > 96 ? replaced.substring(0, 96) : replaced;
    }

    private String buildAnchorRevisionId() {
        String timePart = String.valueOf(System.currentTimeMillis());
        String uuidPart = UUID.randomUUID().toString().replace("-", "").substring(0, 10);
        return timePart + "_" + uuidPart;
    }

    private String normalizeMountedRelativePath(String rawPath, String fallbackName) {
        String candidate = rawPath != null ? rawPath : fallbackName;
        if (candidate == null || candidate.isBlank()) {
            return null;
        }
        String decoded = URLDecoder.decode(candidate, StandardCharsets.UTF_8).trim().replace('\\', '/');
        while (decoded.startsWith("/")) {
            decoded = decoded.substring(1);
        }
        if (decoded.isBlank()) {
            return null;
        }
        Path normalized;
        try {
            normalized = Paths.get(decoded).normalize();
        } catch (Exception ex) {
            return null;
        }
        if (normalized.isAbsolute()) {
            return null;
        }
        String normalizedPath = normalized.toString().replace('\\', '/');
        if (normalizedPath.isBlank() || normalizedPath.equals(".")
                || normalizedPath.startsWith("..")
                || normalizedPath.contains("/../")) {
            return null;
        }
        return normalizedPath;
    }

    private String resolveMainNoteRelativePath(String rawMainNotePath, List<String> uploadedRelativePaths) {
        if (uploadedRelativePaths == null || uploadedRelativePaths.isEmpty()) {
            return null;
        }
        if (rawMainNotePath != null && !rawMainNotePath.isBlank()) {
            String normalized = normalizeMountedRelativePath(rawMainNotePath, null);
            if (normalized != null && uploadedRelativePaths.contains(normalized) && isMarkdownFile(normalized)) {
                return normalized;
            }
        }
        for (String onePath : uploadedRelativePaths) {
            if (isMarkdownFile(onePath)) {
                return onePath;
            }
        }
        return null;
    }

    private String findFirstMarkdownRelativePath(Path revisionDir) {
        if (revisionDir == null || !Files.isDirectory(revisionDir)) {
            return null;
        }
        try (Stream<Path> stream = Files.walk(revisionDir)) {
            Optional<String> found = stream
                    .filter(Files::isRegularFile)
                    .map(path -> revisionDir.relativize(path).toString().replace('\\', '/'))
                    .filter(this::isMarkdownFile)
                    .sorted()
                    .findFirst();
            return found.orElse(null);
        } catch (IOException ex) {
            logger.warn("scan revision markdown failed: dir={} err={}", revisionDir, ex.getMessage());
            return null;
        }
    }

    private String resolveMountedReadNotePath(String rawNotePath, String entryNotePath, String revisionRelativeDir) {
        String normalizedEntry = normalizeMountedRelativePath(entryNotePath, null);
        if (rawNotePath == null || rawNotePath.isBlank()) {
            return normalizedEntry;
        }
        String normalizedRequested = normalizeMountedRelativePath(rawNotePath, null);
        if (normalizedRequested == null) {
            return null;
        }
        String normalizedRevisionDir = normalizeMountedRelativePath(revisionRelativeDir, null);
        if (normalizedRevisionDir != null && !normalizedRevisionDir.isBlank()) {
            if (!normalizedRequested.equals(normalizedRevisionDir)
                    && !normalizedRequested.startsWith(normalizedRevisionDir + "/")) {
                normalizedRequested = normalizeMountedRelativePath(
                        normalizedRevisionDir + "/" + normalizedRequested,
                        null
                );
            }
        }
        if (normalizedRequested == null || !isMarkdownFile(normalizedRequested)) {
            return null;
        }
        return normalizedRequested;
    }

    private String rewriteMountedMarkdownAssetLinks(String markdown, String taskId, String baseRelativeDir) {
        if (markdown == null || markdown.isBlank() || taskId == null || taskId.isBlank()) {
            return markdown != null ? markdown : "";
        }
        String normalizedBase = "";
        if (baseRelativeDir != null && !baseRelativeDir.isBlank()) {
            String normalized = normalizeMountedRelativePath(baseRelativeDir, null);
            if (normalized != null) {
                normalizedBase = normalized;
            }
        }
        Matcher matcher = MARKDOWN_LINK_PATTERN.matcher(markdown);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String originalUrl = matcher.group(2);
            String rewrittenUrl = rewriteMountedResourceUrl(taskId, normalizedBase, originalUrl);
            if (rewrittenUrl == null) {
                matcher.appendReplacement(buffer, Matcher.quoteReplacement(matcher.group(0)));
                continue;
            }
            String replacement = matcher.group(1) + "(" + rewrittenUrl + matcher.group(3) + ")";
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private String rewriteMountedResourceUrl(String taskId, String baseRelativeDir, String rawUrl) {
        String url = stringValueOrNull(rawUrl);
        if (url == null) {
            return null;
        }
        String lower = url.toLowerCase(Locale.ROOT);
        if (lower.startsWith("http://")
                || lower.startsWith("https://")
                || lower.startsWith("data:")
                || lower.startsWith("blob:")
                || lower.startsWith("mailto:")
                || lower.startsWith("tel:")
                || lower.startsWith("#")
                || lower.startsWith("/api/mobile/tasks/")) {
            return null;
        }
        String normalizedPath = normalizeMountedRelativePath(url, null);
        if (normalizedPath == null) {
            return null;
        }
        String fullRelativePath = normalizedPath;
        if (baseRelativeDir != null && !baseRelativeDir.isBlank()) {
            fullRelativePath = baseRelativeDir + "/" + normalizedPath;
        }
        return "/api/mobile/tasks/"
                + URLEncoder.encode(taskId, StandardCharsets.UTF_8)
                + "/asset?path="
                + URLEncoder.encode(fullRelativePath, StandardCharsets.UTF_8);
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

    private List<String> normalizeAnchorIdList(List<String> rawAnchorIds) {
        if (rawAnchorIds == null || rawAnchorIds.isEmpty()) {
            return List.of();
        }
        Set<String> unique = new LinkedHashSet<>();
        for (String rawAnchorId : rawAnchorIds) {
            String anchorId = trimToNullSafe(rawAnchorId);
            if (anchorId != null) {
                unique.add(anchorId);
            }
        }
        return new ArrayList<>(unique);
    }

    private Set<Path> collectAnchorDeleteTargets(
            Path taskRoot,
            Map<String, Object> anchors,
            List<String> anchorIds
    ) {
        Set<Path> targets = new LinkedHashSet<>();
        if (taskRoot == null || anchors == null || anchorIds == null || anchorIds.isEmpty()) {
            return targets;
        }
        for (String anchorId : anchorIds) {
            if (anchorId == null || anchorId.isBlank()) {
                continue;
            }
            String anchorRootRelative = "thinking/anchor_" + sanitizeAnchorDirectoryName(anchorId);
            String normalizedAnchorRoot = normalizeMountedRelativePath(anchorRootRelative, null);
            if (normalizedAnchorRoot != null) {
                Path anchorRoot = taskRoot.resolve(normalizedAnchorRoot).normalize();
                if (anchorRoot.startsWith(taskRoot)) {
                    targets.add(anchorRoot);
                }
            }

            Map<String, Object> anchorRecord = sanitizeSingleAnchorData(anchors.get(anchorId));
            List<Map<String, Object>> revisions = sanitizeAnchorRevisionList(anchorRecord.get("revisions"));
            for (Map<String, Object> revision : revisions) {
                String revisionRelativeDir = stringValueOrNull(revision.get("relativeDir"));
                String normalizedRevisionDir = normalizeMountedRelativePath(revisionRelativeDir, null);
                if (normalizedRevisionDir == null) {
                    continue;
                }
                Path revisionDir = taskRoot.resolve(normalizedRevisionDir).normalize();
                if (revisionDir.startsWith(taskRoot)) {
                    targets.add(revisionDir);
                }
            }

            String mountedPath = stringValueOrNull(anchorRecord.get("mountedPath"));
            String normalizedMountedPath = normalizeMountedRelativePath(mountedPath, null);
            if (normalizedMountedPath != null) {
                Path mountedFile = taskRoot.resolve(normalizedMountedPath).normalize();
                if (mountedFile.startsWith(taskRoot)) {
                    targets.add(mountedFile);
                }
            }
        }
        return targets;
    }

    private int deletePathRecursively(Path target, Path taskRoot) throws IOException {
        if (target == null || taskRoot == null) {
            return 0;
        }
        Path normalizedRoot = taskRoot.toAbsolutePath().normalize();
        Path normalizedTarget = target.toAbsolutePath().normalize();
        if (!normalizedTarget.startsWith(normalizedRoot) || !Files.exists(normalizedTarget)) {
            return 0;
        }
        if (Files.isRegularFile(normalizedTarget) || Files.isSymbolicLink(normalizedTarget)) {
            return Files.deleteIfExists(normalizedTarget) ? 1 : 0;
        }
        final int[] deletedCount = {0};
        Files.walkFileTree(normalizedTarget, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.deleteIfExists(file)) {
                    deletedCount[0] += 1;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }
                if (Files.deleteIfExists(dir)) {
                    deletedCount[0] += 1;
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return deletedCount[0];
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

    private List<TaskView> deduplicateTaskViews(List<TaskView> input) {
        if (input == null || input.isEmpty()) {
            return List.of();
        }
        Map<String, TaskView> deduplicated = new LinkedHashMap<>();
        int anonymousSeq = 0;
        for (TaskView candidate : input) {
            if (candidate == null) {
                continue;
            }
            String key = trimToNullSafe(candidate.taskId);
            if (key == null) {
                key = "__anonymous__" + (++anonymousSeq);
            }
            TaskView existing = deduplicated.get(key);
            if (existing == null) {
                deduplicated.put(key, candidate);
                continue;
            }
            deduplicated.put(key, choosePreferredTaskView(existing, candidate));
        }
        return new ArrayList<>(deduplicated.values());
    }

    private TaskView choosePreferredTaskView(TaskView existing, TaskView candidate) {
        boolean existingRuntimeProcessing = existing.runtimeTask != null && isRunningStatus(existing.status);
        boolean candidateRuntimeProcessing = candidate.runtimeTask != null && isRunningStatus(candidate.status);
        if (existingRuntimeProcessing != candidateRuntimeProcessing) {
            return existingRuntimeProcessing ? existing : candidate;
        }
        if (existing.markdownAvailable != candidate.markdownAvailable) {
            return existing.markdownAvailable ? existing : candidate;
        }
        long existingTs = bestTimestamp(existing);
        long candidateTs = bestTimestamp(candidate);
        if (candidateTs > existingTs) {
            return candidate;
        }
        if (existingTs > candidateTs) {
            return existing;
        }
        if (candidate.runtimeTask != null && existing.runtimeTask == null) {
            return candidate;
        }
        return existing;
    }

    private boolean isRunningStatus(String rawStatus) {
        if (rawStatus == null || rawStatus.isBlank()) {
            return false;
        }
        String status = rawStatus.trim().toUpperCase(Locale.ROOT);
        return "QUEUED".equals(status)
                || "PENDING".equals(status)
                || "PROCESSING".equals(status)
                || "RUNNING".equals(status);
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
        applyTaskDomainAndTopicFromVideoMeta(view, root);
    }

    private void applyTaskDomainAndTopicFromVideoMeta(TaskView view, Path taskRoot) {
        if (view == null || taskRoot == null) {
            return;
        }
        VideoMetaService.VideoMetaSnapshot snapshot = videoMetaService.read(taskRoot);
        if (snapshot.domain != null) {
            view.domain = snapshot.domain;
        }
        if (snapshot.mainTopic != null) {
            view.mainTopic = snapshot.mainTopic;
        }
    }

    private TocMetadata resolveTaskTocMetadata(TaskView task, Path markdownPath) {
        String contentType = null;
        List<Map<String, Object>> bookSectionTree = new ArrayList<>();
        Path taskRoot = resolveTaskRootForToc(task);
        if (taskRoot != null) {
            ObjectNode videoMetaRoot = videoMetaService.readOrCreateNode(taskRoot);
            contentType = normalizeContentTypeToken(readVideoMetaContentType(videoMetaRoot));
            bookSectionTree = readBookSectionTreeFromVideoMeta(videoMetaRoot);
            if (bookSectionTree.isEmpty()) {
                bookSectionTree = readBookSectionTreeFromSemanticUnits(taskRoot);
            }
            if ((contentType == null || contentType.isBlank()) && !bookSectionTree.isEmpty()) {
                contentType = "book";
            }
        }
        if (contentType == null || contentType.isBlank()) {
            contentType = inferTaskContentType(task, markdownPath);
        }
        if (contentType == null || contentType.isBlank()) {
            contentType = "unknown";
        }
        return new TocMetadata(contentType, bookSectionTree);
    }

    private Path resolveTaskRootForToc(TaskView task) {
        try {
            return resolveTaskRootDir(task);
        } catch (Exception ignored) {
            return null;
        }
    }

    private String readVideoMetaContentType(ObjectNode videoMetaRoot) {
        if (videoMetaRoot == null) {
            return null;
        }
        String contentType = trimToNullSafe(videoMetaRoot.path("contentType").asText(null));
        if (contentType == null) {
            contentType = trimToNullSafe(videoMetaRoot.path("content_type").asText(null));
        }
        if (contentType == null) {
            contentType = trimToNullSafe(videoMetaRoot.path("platform").asText(null));
        }
        return contentType;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readBookSectionTreeFromVideoMeta(ObjectNode videoMetaRoot) {
        if (videoMetaRoot == null) {
            return new ArrayList<>();
        }
        JsonNode treeNode = videoMetaRoot.get("bookSectionTree");
        if (treeNode == null || !treeNode.isArray()) {
            treeNode = videoMetaRoot.get("book_section_tree");
        }
        if (treeNode == null || !treeNode.isArray()) {
            return new ArrayList<>();
        }
        try {
            Object converted = objectMapper.convertValue(treeNode, Object.class);
            if (!(converted instanceof List<?> rawList)) {
                return new ArrayList<>();
            }
            List<Map<String, Object>> output = new ArrayList<>();
            for (Object one : rawList) {
                if (one instanceof Map<?, ?> rawMap) {
                    output.add(new LinkedHashMap<>((Map<String, Object>) rawMap));
                }
            }
            return output;
        } catch (Exception ex) {
            logger.debug("parse bookSectionTree from video_meta failed: {}", ex.getMessage());
            return new ArrayList<>();
        }
    }

    private List<Map<String, Object>> readBookSectionTreeFromSemanticUnits(Path taskRoot) {
        if (taskRoot == null) {
            return new ArrayList<>();
        }
        Path metadataPath = taskRoot.resolve("book_semantic_units.json").normalize();
        if (!metadataPath.startsWith(taskRoot) || !Files.isRegularFile(metadataPath)) {
            return new ArrayList<>();
        }
        try {
            JsonNode rootNode = objectMapper.readTree(metadataPath.toFile());
            JsonNode unitsNode = rootNode.path("semantic_units");
            if (!unitsNode.isArray()) {
                return new ArrayList<>();
            }
            List<Map<String, Object>> episodes = new ArrayList<>();
            for (JsonNode unit : unitsNode) {
                if (unit == null || !unit.isObject()) {
                    continue;
                }
                int episodeNo = episodes.size() + 1;
                Integer chapterIndexRaw = parseIntegerNode(unit.get("chapter_index"));
                Integer sectionIndexRaw = parseIntegerNode(unit.get("section_index"));
                Integer startPageRaw = parseIntegerNode(unit.get("start_page"));
                Integer endPageRaw = parseIntegerNode(unit.get("end_page"));
                String sectionSelector = trimToNullSafe(unit.path("section_selector").asText(null));

                int chapterIndex = chapterIndexRaw != null ? chapterIndexRaw : 0;
                int sectionIndex = sectionIndexRaw != null ? sectionIndexRaw : 0;
                int subSectionIndex = parseSubSectionIndexFromSelector(sectionSelector);
                int startPage = startPageRaw != null ? startPageRaw : -1;
                int endPage = endPageRaw != null ? endPageRaw : startPage;
                if (startPage > 0 && endPage > 0 && endPage < startPage) {
                    endPage = startPage;
                }

                Map<String, Object> episode = new LinkedHashMap<>();
                episode.put("index", episodeNo);
                episode.put("chapterIndex", chapterIndex);
                episode.put("sectionIndex", sectionIndex);
                episode.put("subSectionIndex", subSectionIndex > 0 ? subSectionIndex : 1);
                episode.put("chapterTitle", unit.path("chapter_title").asText(""));
                episode.put("title", unit.path("section_title").asText(""));
                episode.put("outlineIndex", buildBookOutlineIndex(chapterIndex, sectionIndex, subSectionIndex, episodeNo));
                episode.put("startPage", startPage);
                episode.put("endPage", endPage);
                if (sectionSelector != null) {
                    episode.put("sectionSelector", sectionSelector);
                }
                episodes.add(episode);
            }
            return buildBookSectionTreeFromEpisodes(episodes);
        } catch (Exception ex) {
            logger.debug("parse book_semantic_units.json failed: path={} err={}", metadataPath, ex.getMessage());
            return new ArrayList<>();
        }
    }

    private List<Map<String, Object>> buildBookSectionTreeFromEpisodes(List<Map<String, Object>> episodes) {
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
            int episodeNo = intValueOrFallback(episode.get("index"), tree.size() + 1);
            int chapterIndex = intValueOrFallback(episode.get("chapterIndex"), 0);
            int sectionIndex = intValueOrFallback(episode.get("sectionIndex"), 0);
            int subSectionIndex = Math.max(1, intValueOrFallback(episode.get("subSectionIndex"), 1));
            int startPage = intValueOrFallback(episode.get("startPage"), -1);
            int endPage = intValueOrFallback(episode.get("endPage"), startPage);
            if (startPage > 0 && endPage > 0 && endPage < startPage) {
                endPage = startPage;
            }
            String chapterTitle = stringValueOrNull(episode.get("chapterTitle"));
            if (chapterTitle == null) {
                chapterTitle = chapterIndex > 0 ? ("Chapter " + chapterIndex) : "Chapter";
            }
            String sectionTitle = stringValueOrNull(episode.get("title"));
            if (sectionTitle == null) {
                sectionTitle = sectionIndex > 0 ? ("Section " + sectionIndex) : ("Section " + episodeNo);
            }
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
            Map<String, Object> leafNode = new LinkedHashMap<>();
            leafNode.put("nodeType", "leaf");
            leafNode.put("episodeNo", Math.max(1, episodeNo));
            leafNode.put("chapterIndex", chapterIndex);
            leafNode.put("sectionIndex", sectionIndex);
            leafNode.put("subSectionIndex", subSectionIndex);
            String outlineIndex = stringValueOrNull(episode.get("outlineIndex"));
            leafNode.put("outlineIndex", outlineIndex != null ? outlineIndex : buildBookOutlineIndex(chapterIndex, sectionIndex, subSectionIndex, episodeNo));
            leafNode.put("title", sectionTitle);
            leafNode.put("startPage", startPage);
            leafNode.put("endPage", endPage);
            String sectionSelector = stringValueOrNull(episode.get("sectionSelector"));
            if (sectionSelector != null) {
                leafNode.put("sectionSelector", sectionSelector);
            }
            sectionChildren.add(leafNode);
        }
        return tree;
    }

    private String inferTaskContentType(TaskView task, Path markdownPath) {
        List<String> candidates = new ArrayList<>();
        if (task != null) {
            candidates.add(task.videoUrl);
            candidates.add(task.resultPath);
            if (task.markdownPath != null) {
                candidates.add(task.markdownPath.toString());
            }
            candidates.add(task.title);
        }
        if (markdownPath != null) {
            candidates.add(markdownPath.toString());
        }
        for (String candidate : candidates) {
            String inferred = inferContentTypeFromTextCandidate(candidate);
            if (inferred != null) {
                return inferred;
            }
        }
        return null;
    }

    private String inferContentTypeFromTextCandidate(String rawCandidate) {
        String text = trimToNullSafe(rawCandidate);
        if (text == null) {
            return null;
        }
        String lower = text.toLowerCase(Locale.ROOT);
        if (lower.matches(".*\\.pdf(?:$|[?#\\s]).*")) {
            return "pdf";
        }
        if (lower.matches(".*\\.epub(?:$|[?#\\s]).*")) {
            return "epub";
        }
        if (lower.matches(".*\\.(md|markdown|txt)(?:$|[?#\\s]).*")) {
            return "document";
        }
        if (lower.matches(".*\\.(mp4|mov|mkv|avi|webm|m4v)(?:$|[?#\\s]).*")) {
            return "video";
        }
        if (lower.contains("ebook") || lower.contains(" book ")) {
            return "book";
        }
        return null;
    }

    private String normalizeContentTypeToken(String rawContentType) {
        String normalized = trimToNullSafe(rawContentType);
        if (normalized == null) {
            return null;
        }
        String lower = normalized.toLowerCase(Locale.ROOT);
        if (lower.contains("pdf")) {
            return "pdf";
        }
        if (lower.contains("epub")) {
            return "epub";
        }
        if (lower.contains("book")) {
            return "book";
        }
        if (lower.contains("markdown") || lower.contains("text") || lower.contains("document")) {
            return "document";
        }
        if (lower.contains("video")) {
            return "video";
        }
        return lower;
    }

    private Integer parseIntegerNode(JsonNode valueNode) {
        if (valueNode == null || valueNode.isNull()) {
            return null;
        }
        try {
            if (valueNode.isNumber()) {
                return valueNode.intValue();
            }
            String text = trimToNullSafe(valueNode.asText(null));
            if (text == null) {
                return null;
            }
            return Integer.parseInt(text);
        } catch (Exception ignored) {
            return null;
        }
    }

    private int parseSubSectionIndexFromSelector(String selector) {
        String normalized = trimToNullSafe(selector);
        if (normalized == null) {
            return -1;
        }
        Matcher matcher = BOOK_SELECTOR_PATTERN.matcher(normalized);
        if (!matcher.find()) {
            return -1;
        }
        String subSectionText = trimToNullSafe(matcher.group(3));
        if (subSectionText == null) {
            return 1;
        }
        try {
            int parsed = Integer.parseInt(subSectionText);
            return parsed > 0 ? parsed : -1;
        } catch (Exception ignored) {
            return -1;
        }
    }

    private String buildBookOutlineIndex(int chapterIndex, int sectionIndex, int subSectionIndex, int fallbackIndex) {
        if (chapterIndex > 0 && sectionIndex > 0 && subSectionIndex > 0) {
            return chapterIndex + "." + sectionIndex + "." + subSectionIndex;
        }
        if (chapterIndex > 0 && sectionIndex > 0) {
            return chapterIndex + "." + sectionIndex;
        }
        return String.valueOf(Math.max(1, fallbackIndex));
    }

    private int intValueOrFallback(Object rawValue, int fallback) {
        if (rawValue == null) {
            return fallback;
        }
        try {
            if (rawValue instanceof Number number) {
                return number.intValue();
            }
            return Integer.parseInt(String.valueOf(rawValue).trim());
        } catch (Exception ignored) {
            return fallback;
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

    private String resolveSubmissionTaskTitle(String rawVideoInput, String normalizedVideoInput) {
        String fromVideoInfo = resolveTitleFromVideoInfo(rawVideoInput, normalizedVideoInput);
        if (fromVideoInfo != null) {
            return fromVideoInfo;
        }
        return deriveTaskTitle(normalizedVideoInput, normalizedVideoInput);
    }

    private String resolveTitleFromVideoInfo(String rawVideoInput, String normalizedVideoInput) {
        if (pythonGrpcClient == null) {
            return null;
        }
        String probeInput = chooseVideoInfoProbeInput(rawVideoInput, normalizedVideoInput);
        if (probeInput == null || probeInput.isBlank()) {
            return null;
        }
        if (!shouldProbeTitleViaVideoInfo(probeInput)) {
            return null;
        }
        int timeoutSec = Math.max(15, mobileVideoInfoTimeoutSeconds);
        String probeTaskId = "MVI_" + UUID.randomUUID();
        try {
            PythonGrpcClient.VideoInfoResult result = pythonGrpcClient.getVideoInfo(
                    probeTaskId,
                    probeInput,
                    timeoutSec
            );
            if (result == null || !result.success) {
                return null;
            }
            String videoTitle = trimToNullSafe(result.videoTitle);
            if (videoTitle != null) {
                return videoTitle;
            }
            return trimToNullSafe(result.canonicalId);
        } catch (Exception ex) {
            logger.debug("mobile submit resolve title via video-info failed: input={} err={}", probeInput, ex.getMessage());
            return null;
        }
    }

    private String chooseVideoInfoProbeInput(String rawInput, String normalizedInput) {
        String raw = trimToNullSafe(rawInput);
        String normalized = trimToNullSafe(normalizedInput);
        if (raw != null) {
            String lowerRaw = raw.toLowerCase(Locale.ROOT);
            if (lowerRaw.contains("http://") || lowerRaw.contains("https://")) {
                return raw;
            }
        }
        if (normalized != null) {
            return normalized;
        }
        return raw;
    }

    private boolean shouldProbeTitleViaVideoInfo(String probeInput) {
        String normalizedProbeInput = trimToNullSafe(probeInput);
        if (normalizedProbeInput == null) {
            return false;
        }
        String inferredContentType = normalizeContentTypeToken(
                inferContentTypeFromTextCandidate(normalizedProbeInput)
        );
        if ("video".equals(inferredContentType)) {
            return true;
        }
        if ("pdf".equals(inferredContentType)
                || "epub".equals(inferredContentType)
                || "document".equals(inferredContentType)
                || "book".equals(inferredContentType)) {
            return false;
        }
        String lower = normalizedProbeInput.toLowerCase(Locale.ROOT);
        return lower.contains("http://") || lower.contains("https://");
    }

    private String instantToText(Instant instant) {
        return instant == null ? "" : instant.toString();
    }




    private String normalizeVideoInput(String rawVideoInput) {
        return VideoInputNormalizerBridge.normalizeVideoInput(rawVideoInput);
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

    private ResponseEntity<Map<String, Object>> buildUploadSubmissionResponse(
            Path savedVideoPath,
            String safeFileName,
            String normalizedUserId,
            String outputDir,
            TaskQueueManager.Priority taskPriority,
            String chapterSelector,
            String sectionSelector,
            Boolean splitByChapter,
            Boolean splitBySection,
            Integer pageOffset,
            Boolean probeOnly,
            Optional<FileReuseService.FileFingerprint> fingerprintOpt,
            boolean reusedUpload
    ) {
        if (Boolean.TRUE.equals(probeOnly)) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("probeOnly", true);
            payload.put("reused", reusedUpload);
            payload.put("normalizedVideoUrl", savedVideoPath.toString());
            payload.put("uploadedFileName", safeFileName);
            payload.put("message", reusedUpload ? "file reused for probe" : "file uploaded for probe");
            appendFingerprintPayload(payload, fingerprintOpt);
            appendProbeCachePayload(payload, fingerprintOpt);
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
                savedVideoPath.toString(),
                normalizeOutputDir(outputDir),
                taskPriority,
                null,
                bookOptions
        );
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("reused", reusedUpload);
        payload.put("taskId", task.taskId);
        payload.put("status", task.status.name());
        payload.put("normalizedVideoUrl", savedVideoPath.toString());
        payload.put("uploadedFileName", safeFileName);
        payload.put("message", reusedUpload
                ? "file reused; task submitted and queued"
                : "video uploaded; task submitted and queued");
        appendFingerprintPayload(payload, fingerprintOpt);
        return ResponseEntity.ok(payload);
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

    private TaskQueueManager.BookProcessingOptions buildBookProcessingOptions(
            String chapterSelector,
            String sectionSelector,
            Boolean splitByChapter,
            Boolean splitBySection,
            Integer pageOffset
    ) {
        String normalizedChapterSelector = trimToNullSafe(chapterSelector);
        String normalizedSectionSelector = trimToNullSafe(sectionSelector);
        if (normalizedChapterSelector == null && normalizedSectionSelector == null
                && splitByChapter == null && splitBySection == null
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

    private String normalizeUploadId(String rawUploadId) {
        String normalized = rawUploadId == null ? "" : rawUploadId.trim();
        if (!SAFE_UPLOAD_ID_PATTERN.matcher(normalized).matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "invalid uploadId");
        }
        return normalized;
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
        if (name.endsWith(".pdf")) return MediaType.APPLICATION_PDF;
        if (name.endsWith(".txt")) return MediaType.TEXT_PLAIN;
        if (name.endsWith(".epub")) return MediaType.parseMediaType("application/epub+zip");
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

    private static class TocMetadata {
        private final String contentType;
        private final List<Map<String, Object>> bookSectionTree;

        private TocMetadata(String contentType, List<Map<String, Object>> bookSectionTree) {
            this.contentType = contentType != null ? contentType : "unknown";
            this.bookSectionTree = bookSectionTree != null ? bookSectionTree : List.of();
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
        public Map<String, Object> anchors = new LinkedHashMap<>();
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
        public Map<String, Object> anchors;
    }

    public static class AnchorSyncRequest {
        public String path;
        public String mainNotePath;
        public List<AnchorSyncOperation> operations;
    }

    public static class AnchorBatchDeleteRequest {
        public String path;
        public List<String> anchorIds;
        public Boolean removeFiles;
    }

    public static class AnchorSyncOperation {
        public String op;
        public String relativePath;
        public String content;
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
        public String chapterSelector;
        public String sectionSelector;
        public Boolean splitByChapter;
        public Boolean splitBySection;
        public Integer pageOffset;
    }

    public static class ChunkUploadCompleteRequest {
        public String uploadId;
        public String userId;
        public String outputDir;
        public String priority;
        public String chapterSelector;
        public String sectionSelector;
        public Boolean splitByChapter;
        public Boolean splitBySection;
        public Integer pageOffset;
        public Boolean probeOnly;
        public String fileMd5;
        public String fileExt;
    }

    public static class UploadReuseCheckRequest {
        public String userId;
        public String outputDir;
        public String priority;
        public String chapterSelector;
        public String sectionSelector;
        public Boolean splitByChapter;
        public Boolean splitBySection;
        public Integer pageOffset;
        public Boolean probeOnly;
        public String fileName;
        public Long fileSize;
        public String fileMd5;
        public String fileExt;
        public Boolean autoSubmit;
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
        private String domain;
        private String mainTopic;
        private double progress;
        private TaskEntry runtimeTask;
        private String collectionId;
        private Integer episodeNo;
        private String episodeTitle;
        private String collectionTitle;
        private Integer totalEpisodes;
    }
}
