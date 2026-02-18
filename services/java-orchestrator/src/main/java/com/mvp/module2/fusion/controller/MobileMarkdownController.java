package com.mvp.module2.fusion.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.common.TaskDisplayNameResolver;
import com.mvp.module2.fusion.common.UserFacingErrorMapper;
import com.mvp.module2.fusion.common.VideoInputNormalizer;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskEntry;
import com.mvp.module2.fusion.queue.TaskQueueManager.TaskStatus;
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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

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
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
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
 * 绉诲姩绔?Markdown 鏌ョ湅 API銆? * 璁捐鐩爣锛? * 1. 澶嶇敤鐜版湁 TaskQueueManager锛屼笉寮曞叆棰濆鎸佷箙鍖栵紱
 * 2. 浠诲姟鍒楄〃鍚屾椂鑱氬悎杩愯鏃朵换鍔′笌 var/storage/storage 鍘嗗彶浠诲姟锛? * 3. 涓ユ牸鍋氳矾寰勫綊涓€鍜岀洰褰曡竟鐣屾牎楠岋紝閬垮厤璺緞绌胯秺銆? */
@RestController
@RequestMapping("/api/mobile")
public class MobileMarkdownController {

    private static final Logger logger = LoggerFactory.getLogger(MobileMarkdownController.class);
    private static final String STORAGE_TASK_PREFIX = "storage:";
    private static final String DEFAULT_MARKDOWN_NAME = "enhanced_output.md";
    private static final int MARKDOWN_SCAN_DEPTH = 4;
    private static final String META_FILE_NAME = "mobile_task_meta.json";
    private static final String META_DEFAULT_NOTE_KEY = "__default__";
    private static final Pattern UNSAFE_FILENAME_CHARS = Pattern.compile("[^A-Za-z0-9._-]");
    private static final Set<String> ALLOWED_VIDEO_EXTENSIONS = Set.of(".mp4", ".mov", ".mkv", ".avi", ".webm", ".m4v");
    private static final long MAX_UPLOAD_FILE_BYTES = 2L * 1024L * 1024L * 1024L;

    @Autowired
    private TaskQueueManager taskQueueManager;

    @Autowired
    private com.mvp.module2.fusion.service.StorageTaskCacheService storageTaskCacheService;

    @Value("${task.upload.dir:var/uploads}")
    private String uploadDir;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @GetMapping("/tasks")
    public ResponseEntity<Map<String, Object>> listTasks(
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "pageSize", defaultValue = "20") int pageSize
    ) {
        // 1. 鑾峰彇杩愯鏃朵换鍔?(閫氬父鏁伴噺寰堝皯锛岀洿鎺ュ叏閮ㄨ幏鍙?
        List<TaskEntry> runtimeTasks = taskQueueManager.getAllTasks();
        // 2. 鑾峰彇瀛樺偍浠诲姟 (鍒嗛〉)
        com.mvp.module2.fusion.service.StorageTaskCacheService.PagedResult storageResult = 
                storageTaskCacheService.getTasks(page, pageSize);

        // 3. 鑱氬悎閫昏緫
        // 绛栫暐锛氳繍琛屾椂浠诲姟鎬绘槸鏄剧ず鍦ㄦ渶鍓嶉潰锛堝洜涓洪€氬父鏄渶鏂扮殑锛夛紝鐒跺悗鍐嶈拷鍔犲瓨鍌ㄤ换鍔＄殑鍒嗛〉缁撴灉銆?        // 娉ㄦ剰锛氳繖绉嶇畝鍗曠殑鑱氬悎鍦ㄧ炕椤垫椂鍙兘浼氭湁杞诲井鐨勪笉涓€鑷达紙濡傛灉杩愯鏃朵换鍔℃濂藉湪缈婚〉鏈熼棿瀹屾垚骞跺綊妗ｏ級锛?        // 浣嗗浜庣Щ鍔ㄧ绠€鍗曠殑鍒楄〃娴忚鏉ヨ鏄彲浠ユ帴鍙楃殑锛屼笖閬垮厤浜嗗鏉傜殑鍏ㄩ噺鎺掑簭寮€閿€銆?        
        List<TaskView> finalViewList = new ArrayList<>();

        if (page == 0) {
            for (TaskEntry runtimeTask : runtimeTasks) {
                finalViewList.add(fromRuntimeTask(runtimeTask));
            }
        }
        
        for (com.mvp.module2.fusion.service.StorageTaskCacheService.CachedTask cached : storageResult.tasks) {
            finalViewList.add(fromCachedTask(cached));
        }

        List<Map<String, Object>> taskList = new ArrayList<>(finalViewList.size());
        for (TaskView task : finalViewList) {
            taskList.add(toListItem(task));
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("tasks", taskList);
        // 鎬绘暟 = 杩愯鏃朵换鍔?+ 瀛樺偍浠诲姟鎬绘暟
        response.put("totalCount", runtimeTasks.size() + storageResult.totalCount);
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
                    "message", "璇锋眰浣撲笉鑳戒负绌?"
            ));
        }

        String normalizedVideoInput = normalizeVideoInput(request.videoUrl);
        if (normalizedVideoInput.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "videoUrl 涓嶈兘涓虹┖"
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

        return ResponseEntity.ok(Map.of(
                "success", true,
                "taskId", task.taskId,
                "status", task.status.name(),
                "normalizedVideoUrl", normalizedVideoInput,
                "message", "浠诲姟宸叉彁浜わ紝姝ｅ湪鎺掗槦涓?"
        ));
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
                    "message", "videoFile 涓嶈兘涓虹┖"
            ));
        }
        if (videoFile.getSize() > MAX_UPLOAD_FILE_BYTES) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "涓婁紶鏂囦欢杩囧ぇ锛屽綋鍓嶉檺鍒?2048MB"
            ));
        }

        String safeFileName = sanitizeUploadFileName(videoFile.getOriginalFilename());
        if (!hasSupportedVideoExtension(safeFileName)) {
            return ResponseEntity.badRequest().body(Map.of(
                    "success", false,
                    "message", "浠呮敮鎸佸父瑙佽棰戞牸寮忥細mp4/mov/mkv/avi/webm/m4v"
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
                    "message", "瑙嗛宸蹭笂浼狅紝浠诲姟宸叉彁浜わ紝姝ｅ湪鎺掗槦涓?"
            ));
        } catch (IOException e) {
            logger.error("绉诲姩绔笂浼犺棰戜繚瀛樺け璐?", e);
            return ResponseEntity.status(503).body(Map.of(
                    "success", false,
                    "message", UserFacingErrorMapper.busyMessage()
            ));
        }
    }

    @GetMapping("/tasks/{taskId}/markdown")
    public ResponseEntity<?> getTaskMarkdown(@PathVariable String taskId) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "浠诲姟涓嶅瓨鍦?"));
        }

        ResolvedMarkdown resolved;
        try {
            resolved = resolveMarkdown(task);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.status(404).body(Map.of("message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("璇诲彇 markdown 澶辫触: taskId={} err={}", taskId, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "璇诲彇 markdown 澶辫触"));
        }

        try {
            String markdown = Files.readString(resolved.markdownPath, StandardCharsets.UTF_8);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("taskId", task.taskId);
            response.put("title", task.title);
            response.put("status", task.status);
            response.put("markdown", markdown);
            response.put("markdownPath", resolved.markdownPath.toString());
            response.put("baseDir", resolved.baseDir.toString());
            response.put("assetEndpointTemplate", "/api/mobile/tasks/" + task.taskId + "/asset?path={path}");
            return ResponseEntity.ok(response);
        } catch (IOException ex) {
            logger.warn("璇诲彇 markdown 鍐呭澶辫触: taskId={} path={} err={}", taskId, resolved.markdownPath, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "璇诲彇 markdown 鍐呭澶辫触"));
        }
    }

    @GetMapping("/tasks/{taskId}/markdown/by-path")
    public ResponseEntity<?> getTaskMarkdownByRelativePath(
            @PathVariable String taskId,
            @RequestParam("path") String rawPath
    ) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "浠诲姟涓嶅瓨鍦?"));
        }
        if (rawPath == null || rawPath.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("message", "缂哄皯 path 鍙傛暟"));
        }

        ResolvedMarkdown resolved;
        try {
            resolved = resolveMarkdown(task);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.status(404).body(Map.of("message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("璇诲彇 markdown 澶辫触: taskId={} err={}", taskId, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "璇诲彇 markdown 澶辫触"));
        }

        String decodedPath = URLDecoder.decode(rawPath, StandardCharsets.UTF_8);
        Path target = resolveAssetTargetPath(resolved.baseDir, decodedPath);
        if (!target.startsWith(resolved.baseDir)) {
            return ResponseEntity.status(400).body(Map.of("message", "闈炴硶璺緞"));
        }
        if (!Files.exists(target) || !Files.isRegularFile(target)) {
            return ResponseEntity.status(404).body(Map.of("message", "鏂囦欢涓嶅瓨鍦?"));
        }
        if (!isMarkdownFile(target.getFileName().toString())) {
            return ResponseEntity.status(400).body(Map.of("message", "鐩爣涓嶆槸 markdown 鏂囦欢"));
        }

        try {
            String markdown = Files.readString(target, StandardCharsets.UTF_8);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("taskId", task.taskId);
            response.put("title", target.getFileName().toString());
            response.put("status", task.status);
            response.put("markdown", markdown);
            response.put("markdownPath", target.toString());
            response.put("baseDir", resolved.baseDir.toString());
            response.put("assetEndpointTemplate", "/api/mobile/tasks/" + task.taskId + "/asset?path={path}");
            return ResponseEntity.ok(response);
        } catch (IOException ex) {
            logger.warn("璇诲彇鐩稿 markdown 澶辫触: taskId={} path={} err={}", taskId, rawPath, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "璇诲彇 markdown 澶辫触"));
        }
    }

    @PutMapping("/tasks/{taskId}/markdown")
    public ResponseEntity<?> updateTaskMarkdown(
            @PathVariable String taskId,
            @RequestBody MarkdownUpdateRequest request
    ) {
        if (request == null || request.markdown == null) {
            return ResponseEntity.badRequest().body(Map.of("message", "缂哄皯 markdown 鍐呭"));
        }

        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "浠诲姟涓嶅瓨鍦?"));
        }

        ResolvedMarkdown resolved;
        try {
            resolved = resolveMarkdown(task);
        } catch (Exception ex) {
            return ResponseEntity.status(404).body(Map.of("message", "鏈壘鍒板彲缂栬緫 markdown"));
        }

        Path target = resolved.markdownPath;
        if (request.path != null && !request.path.isBlank()) {
            String decodedPath = URLDecoder.decode(request.path, StandardCharsets.UTF_8);
            target = resolveAssetTargetPath(resolved.baseDir, decodedPath);
            if (!target.startsWith(resolved.baseDir)) {
                return ResponseEntity.status(400).body(Map.of("message", "闈炴硶璺緞"));
            }
            if (!isMarkdownFile(target.getFileName().toString())) {
                return ResponseEntity.status(400).body(Map.of("message", "鐩爣涓嶆槸 markdown 鏂囦欢"));
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
            logger.warn("鍐欏叆 markdown 澶辫触: taskId={} path={} err={}", taskId, target, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "鍐欏叆 markdown 澶辫触"));
        }
    }

    @GetMapping("/tasks/{taskId}/meta")
    public ResponseEntity<?> getTaskMeta(
            @PathVariable String taskId,
            @RequestParam(value = "path", required = false) String rawPath
    ) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "浠诲姟涓嶅瓨鍦?"));
        }
        Path taskRoot;
        try {
            taskRoot = resolveTaskRootDir(task);
        } catch (Exception ex) {
            return ResponseEntity.status(404).body(Map.of("message", "鏈壘鍒颁换鍔＄洰褰?"));
        }

        String noteKey = normalizeMetaNoteKey(taskRoot, rawPath);
        TaskMetaFile meta = readTaskMeta(taskRoot);
        NoteMeta noteMeta = meta.notesByMarkdown.getOrDefault(noteKey, new NoteMeta());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("taskId", task.taskId);
        payload.put("taskTitle", meta.taskTitle != null ? meta.taskTitle : "");
        payload.put("pathKey", noteKey);
        payload.put("favorites", noteMeta.favorites != null ? noteMeta.favorites : Map.of());
        payload.put("comments", noteMeta.comments != null ? sanitizeComments(noteMeta.comments) : Map.of());
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
            return ResponseEntity.status(404).body(Map.of("message", "浠诲姟涓嶅瓨鍦?"));
        }
        Path taskRoot;
        try {
            taskRoot = resolveTaskRootDir(task);
        } catch (Exception ex) {
            return ResponseEntity.status(404).body(Map.of("message", "鏈壘鍒颁换鍔＄洰褰?"));
        }

        TaskMetaFile meta = readTaskMeta(taskRoot);
        if (request != null && request.taskTitle != null) {
            String title = request.taskTitle.trim();
            meta.taskTitle = title.isEmpty() ? null : title;
        }

        String noteKey = normalizeMetaNoteKey(taskRoot, request != null ? request.path : null);
        NoteMeta noteMeta = meta.notesByMarkdown.getOrDefault(noteKey, new NoteMeta());
        if (request != null && request.favorites != null) {
            noteMeta.favorites = sanitizeFavorites(request.favorites);
        }
        if (request != null && request.comments != null) {
            noteMeta.comments = new LinkedHashMap<>(sanitizeComments(request.comments));
        }
        if ((noteMeta.favorites == null || noteMeta.favorites.isEmpty())
                && (noteMeta.comments == null || noteMeta.comments.isEmpty())) {
            meta.notesByMarkdown.remove(noteKey);
        } else {
            meta.notesByMarkdown.put(noteKey, noteMeta);
        }

        if (!writeTaskMeta(taskRoot, meta)) {
            return ResponseEntity.status(500).body(Map.of("message", "鍐欏叆浠诲姟鍏冩暟鎹け璐?"));
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("success", true);
        payload.put("taskId", task.taskId);
        payload.put("taskTitle", meta.taskTitle != null ? meta.taskTitle : "");
        payload.put("pathKey", noteKey);
        payload.put("favorites", noteMeta.favorites != null ? noteMeta.favorites : Map.of());
        payload.put("comments", noteMeta.comments != null ? sanitizeComments(noteMeta.comments) : Map.of());
        payload.put("metaPath", taskRoot.resolve(META_FILE_NAME).toString());
        payload.put("updatedAt", Instant.now().toString());
        return ResponseEntity.ok(payload);
    }

    @GetMapping("/tasks/{taskId}/asset")
    public ResponseEntity<?> getTaskAsset(
            @PathVariable String taskId,
            @RequestParam("path") String rawPath,
            @RequestHeader(value = HttpHeaders.RANGE, required = false) String rangeHeader
    ) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            return ResponseEntity.status(404).body(Map.of("message", "浠诲姟涓嶅瓨鍦?"));
        }
        if (rawPath == null || rawPath.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("message", "缂哄皯 path 鍙傛暟"));
        }

        ResolvedMarkdown resolved;
        try {
            resolved = resolveMarkdown(task);
        } catch (Exception ex) {
            return ResponseEntity.status(404).body(Map.of("message", "鏈壘鍒?markdown 鍩虹洰褰?"));
        }

        String decodedPath = URLDecoder.decode(rawPath, StandardCharsets.UTF_8);
        Path target = resolveAssetTargetPath(resolved.baseDir, decodedPath);
        if (!target.startsWith(resolved.baseDir)) {
            return ResponseEntity.status(400).body(Map.of("message", "闈炴硶璺緞"));
        }
        if (!Files.exists(target) || !Files.isRegularFile(target)) {
            return ResponseEntity.status(404).body(Map.of("message", "鏂囦欢涓嶅瓨鍦?"));
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
                    logger.warn("澶勭悊 Range 璇锋眰澶辫触锛屽洖閫€鍏ㄩ噺鍝嶅簲: taskId={} path={} range={} err={}",
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
            logger.warn("璇诲彇璧勬簮鏂囦欢澶辫触: taskId={} path={} err={}", taskId, target, ex.getMessage());
            return ResponseEntity.status(500).body(Map.of("message", "璇诲彇璧勬簮鏂囦欢澶辫触"));
        }
    }

    @GetMapping("/tasks/{taskId}/export")
    public ResponseEntity<StreamingResponseBody> exportTaskBundle(@PathVariable String taskId) {
        TaskView task = resolveTaskView(taskId);
        if (task == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "浠诲姟涓嶅瓨鍦?");
        }
        Path taskRoot;
        try {
            taskRoot = resolveTaskRootDir(task);
        } catch (Exception ex) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "鏈壘鍒颁换鍔＄洰褰?", ex);
        }
        if (!Files.exists(taskRoot) || !Files.isDirectory(taskRoot)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "浠诲姟鐩綍涓嶅瓨鍦?");
        }

        final List<ExportFileEntry> exportEntries;
        try {
            exportEntries = collectExportEntries(taskRoot);
        } catch (IOException ex) {
            logger.warn("瀵煎嚭浠诲姟澶辫触: taskId={} root={} err={}", taskId, taskRoot, ex.getMessage());
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "??????", ex);
        }
        if (exportEntries.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "鏈壘鍒板彲瀵煎嚭鐨?markdown/瑙嗛/鎴浘绱犳潗");
        }

        String filename = buildSafeExportFilename(task.taskId);
        StreamingResponseBody body = outputStream -> {
            try (ZipOutputStream zos = new ZipOutputStream(outputStream, StandardCharsets.UTF_8)) {
                ExportZipResult zipResult = writeTaskExportZipStreaming(exportEntries, zos);
                zos.finish();
                logger.info("娴佸紡瀵煎嚭浠诲姟瀹屾垚: taskId={} root={} exported={} skipped={}",
                        taskId, taskRoot, zipResult.exportedCount, zipResult.skippedCount);
            } catch (IOException ex) {
                logger.warn("娴佸紡瀵煎嚭浠诲姟澶辫触: taskId={} root={} err={}", taskId, taskRoot, ex.getMessage());
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
        item.put("taskId", task.taskId);
        item.put("title", task.title != null ? task.title : task.taskId);
        item.put("metaTitle", task.metaTitle != null ? task.metaTitle : "");
        item.put("titleSource", resolveTaskTitleSource(task));
        item.put("videoUrl", task.videoUrl != null ? task.videoUrl : "");
        item.put("status", task.status != null ? task.status : "");
        item.put("createdAt", instantToText(task.createdAt));
        item.put("completedAt", instantToText(task.completedAt));
        item.put("resultPath", task.resultPath != null ? task.resultPath : "");
        item.put("markdownPath", task.markdownPath != null ? task.markdownPath.toString() : "");
        item.put("markdownAvailable", task.markdownAvailable);
        item.put("source", task.storageTask ? "storage" : "runtime");
        item.put("storageKey", task.storageKey != null ? task.storageKey : "");
        item.put("progress", task.progress);
        item.put("statusMessage", task.statusMessage != null ? task.statusMessage : "");
        return item;
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

        String storageKey = null;
        if (taskId.startsWith(STORAGE_TASK_PREFIX)) {
            storageKey = taskId.substring(STORAGE_TASK_PREFIX.length());
        } else if (isSafeStorageKey(taskId)) {
            // 兼容旧链接：未加 storage: 前缀时，回退按目录名解析。
            storageKey = taskId;
        }
        if (storageKey == null || storageKey.isBlank()) {
            return null;
        }
        
        // 浣跨敤缂撳瓨鏈嶅姟鏌ユ壘
        Optional<com.mvp.module2.fusion.service.StorageTaskCacheService.CachedTask> cachedOpt = 
                storageTaskCacheService.getTask(storageKey);
        return cachedOpt.map(this::fromCachedTask).orElse(null);
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
                    // 忽略异常，继续返回运行态任务。
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
        throw new IllegalArgumentException("浠诲姟灏氭湭鐢熸垚 Markdown");
    }

    private ResolvedMarkdown resolveMarkdown(TaskEntry task) throws IOException {
        if (task == null || task.resultPath == null || task.resultPath.isBlank()) {
            throw new IllegalArgumentException("浠诲姟灏氭湭鐢熸垚 Markdown");
        }
        Path resultPath = Paths.get(task.resultPath).toAbsolutePath().normalize();
        if (!Files.exists(resultPath)) {
            throw new IllegalArgumentException("缁撴灉璺緞涓嶅瓨鍦? " + resultPath);
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
            throw new IllegalArgumentException("鏈壘鍒板彲鐢?markdown 鐩綍");
        }

        if (preferredPath != null && !preferredPath.isBlank()) {
            try {
                Path preferred = Paths.get(preferredPath).toAbsolutePath().normalize();
                if (Files.isRegularFile(preferred) && preferred.startsWith(searchRoot) && isMarkdownFile(preferred.getFileName().toString())) {
                    return new ResolvedMarkdown(preferred, preferred.getParent());
                }
            } catch (Exception ignored) {
                // 回退到目录扫描。
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
            throw new IllegalArgumentException("鏈壘鍒?markdown 鏂囦欢");
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
                // 绌烘枃浠堕€氬父鏉ヨ嚜鍘嗗彶鍐欏叆涓柇锛屾寜鎹熷潖澶勭悊骞跺垹闄わ紝閬垮厤鍚庣画鎸佺画 EOF 鍣煶銆?
                logger.warn("浠诲姟鍏冩暟鎹枃浠朵负绌猴紝鎸夋崯鍧忓鐞嗗苟閲嶅缓: {}", metaPath);
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
            for (Map.Entry<String, NoteMeta> entry : loaded.notesByMarkdown.entrySet()) {
                if (entry.getValue() == null) {
                    entry.setValue(new NoteMeta());
                    continue;
                }
                if (entry.getValue().favorites == null) {
                    entry.getValue().favorites = new LinkedHashMap<>();
                }
                if (entry.getValue().comments == null) {
                    entry.getValue().comments = new LinkedHashMap<>();
                } else {
                    entry.getValue().comments = new LinkedHashMap<>(sanitizeComments(entry.getValue().comments));
                }
            }
            return loaded;
        } catch (Exception ex) {
            logger.warn("璇诲彇浠诲姟鍏冩暟鎹け璐? {} err={}", metaPath, ex.getMessage());
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
            // 鍏堝啓涓存椂鏂囦欢鍐嶆浛鎹紝閬垮厤搴忓垪鍖栧紓甯告椂鎶婄洰鏍囨枃浠舵埅鏂负绌恒€?            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmpPath.toFile(), meta);
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
                // 临时文件清理失败不影响主流程，仅保留原始异常日志。
            }
                logger.warn("鍐欏叆浠诲姟鍏冩暟鎹け璐? {} err={}", metaPath, ex.getMessage());
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
            // 非法路径按普通字符串处理。
        }
        while (decoded.startsWith("/") || decoded.startsWith("\\")) {
            decoded = decoded.substring(1);
        }
        decoded = decoded.replace('\\', '/');
        return decoded.isBlank() ? META_DEFAULT_NOTE_KEY : decoded;
    }

    private Map<String, Boolean> sanitizeFavorites(Map<String, Boolean> input) {
        Map<String, Boolean> output = new LinkedHashMap<>();
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
                logger.warn("閬嶅巻瀵煎嚭鐩綍澶辫触锛屽凡璺宠繃: root={} path={} err={}",
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
                    throw new IOException("鏍稿績鏂囦欢鍐欏叆 ZIP 澶辫触: " + entry.entryName, ex);
                }
                skippedCount += 1;
                logger.warn("瀵煎嚭 ZIP 鏃惰烦杩囨枃浠? entry={} path={} err={}",
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
                // 回退到目录名。
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
            logger.info("蹇界暐瀹㈡埛绔紭鍏堢骇鍙傛暟: user={} priority={}", normalizedUserId, rawPriority);
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
            throw new IOException("闈炴硶涓婁紶璺緞");
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
            // 继续走扩展名兜底。
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
        public Map<String, NoteMeta> notesByMarkdown = new LinkedHashMap<>();
    }

    private static class NoteMeta {
        public Map<String, Boolean> favorites = new LinkedHashMap<>();
        public Map<String, Object> comments = new LinkedHashMap<>();
    }

    public static class MarkdownUpdateRequest {
        public String path;
        public String markdown;
    }

    public static class TaskMetaUpdateRequest {
        public String path;
        public String taskTitle;
        public Map<String, Boolean> favorites;
        public Map<String, Object> comments;
    }

    public static class TaskSubmitRequest {
        public String userId;
        public String videoUrl;
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
    }
}
