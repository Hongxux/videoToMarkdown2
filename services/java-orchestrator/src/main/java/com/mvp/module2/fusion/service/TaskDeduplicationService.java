package com.mvp.module2.fusion.service;

import com.mvp.module2.fusion.common.VideoInputNormalizer;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

@Service
public class TaskDeduplicationService {

    private static final Logger logger = LoggerFactory.getLogger(TaskDeduplicationService.class);
    private static final int REUSABLE_MARKDOWN_SCAN_DEPTH = 4;
    private final ConcurrentHashMap<String, String> activeTaskOwners = new ConcurrentHashMap<>();

    @Autowired(required = false)
    private TaskStateRepository taskStateRepository;

    public NormalizedTaskInput normalizeTaskInput(TaskQueueManager.TaskEntry task) {
        String rawInput = task != null && task.videoUrl != null ? task.videoUrl.trim() : "";
        String normalizedVideoUrl = VideoInputNormalizer.normalizeVideoInput(rawInput);
        if (normalizedVideoUrl.isBlank()) {
            normalizedVideoUrl = rawInput;
        }
        String normalizedVideoKey = buildNormalizedVideoKey(normalizedVideoUrl);
        return new NormalizedTaskInput(rawInput, normalizedVideoUrl, normalizedVideoKey);
    }

    public String registerOrGetActiveOwner(String normalizedVideoKey, String taskId) {
        if (normalizedVideoKey == null || normalizedVideoKey.isBlank() || taskId == null || taskId.isBlank()) {
            return "";
        }
        String existing = activeTaskOwners.putIfAbsent(normalizedVideoKey, taskId);
        return existing != null ? existing : taskId;
    }

    public void releaseActiveOwner(String normalizedVideoKey, String taskId) {
        if (normalizedVideoKey == null || normalizedVideoKey.isBlank() || taskId == null || taskId.isBlank()) {
            return;
        }
        activeTaskOwners.remove(normalizedVideoKey, taskId);
    }

    public Optional<TaskStateRepository.PersistedTaskRecord> findReusablePersistedTask(
            String normalizedVideoKey,
            String excludeTaskId
    ) {
        if (taskStateRepository == null || normalizedVideoKey == null || normalizedVideoKey.isBlank()) {
            return Optional.empty();
        }
        List<TaskStateRepository.PersistedTaskRecord> candidates =
                taskStateRepository.findReusableTasksByNormalizedVideoKey(normalizedVideoKey, excludeTaskId);
        for (TaskStateRepository.PersistedTaskRecord candidate : candidates) {
            if (isPersistedTaskReusable(candidate)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    public String buildNormalizedVideoKey(String rawInput) {
        String normalizedInput = rawInput != null ? rawInput.trim() : "";
        if (normalizedInput.isBlank()) {
            return "";
        }
        if (VideoInputNormalizer.looksLikeLocalPath(normalizedInput)) {
            try {
                Path path = Paths.get(normalizedInput).toAbsolutePath().normalize();
                return path.toString().replace('\\', '/').toLowerCase(Locale.ROOT);
            } catch (Exception error) {
                return normalizedInput.replace('\\', '/').toLowerCase(Locale.ROOT);
            }
        }
        try {
            URI uri = URI.create(normalizedInput);
            String host = uri.getHost() != null ? uri.getHost().trim().toLowerCase(Locale.ROOT) : "";
            if (host.startsWith("www.")) {
                host = host.substring(4);
            }
            String path = uri.getPath() != null ? uri.getPath().trim() : "";
            if (path.endsWith("/") && path.length() > 1) {
                path = path.substring(0, path.length() - 1);
            }
            String query = uri.getRawQuery() != null ? uri.getRawQuery().trim() : "";
            StringBuilder key = new StringBuilder();
            key.append(host);
            key.append(path);
            if (!query.isBlank()) {
                key.append('?').append(query);
            }
            return key.toString();
        } catch (Exception error) {
            String lowered = normalizedInput.toLowerCase(Locale.ROOT);
            if (lowered.startsWith("https://")) {
                lowered = lowered.substring("https://".length());
            } else if (lowered.startsWith("http://")) {
                lowered = lowered.substring("http://".length());
            }
            if (lowered.startsWith("www.")) {
                lowered = lowered.substring(4);
            }
            return lowered;
        }
    }

    private boolean isPersistedTaskReusable(TaskStateRepository.PersistedTaskRecord record) {
        if (record == null) {
            return false;
        }
        String normalizedStatus = record.status != null ? record.status.trim().toUpperCase(Locale.ROOT) : "";
        if ("QUEUED".equals(normalizedStatus)
                || "PROBING".equals(normalizedStatus)
                || "PROCESSING".equals(normalizedStatus)) {
            return true;
        }
        if (!"COMPLETED".equals(normalizedStatus)) {
            return false;
        }
        boolean reusable = hasReusableCompletedOutput(record);
        if (!reusable) {
            logger.warn(
                    "Skip stale completed task during dedup: taskId={} normalizedVideoKey={} resultPath={} outputDir={}",
                    record.taskId,
                    record.normalizedVideoKey,
                    record.resultPath,
                    record.outputDir
            );
        }
        return reusable;
    }

    private boolean hasReusableCompletedOutput(TaskStateRepository.PersistedTaskRecord record) {
        Path resultPath = toNormalizedPath(record != null ? record.resultPath : null);
        if (resultPath != null && containsReusableMarkdown(resultPath)) {
            return true;
        }
        Path outputDir = toNormalizedPath(record != null ? record.outputDir : null);
        return outputDir != null && containsReusableMarkdown(outputDir);
    }

    private Path toNormalizedPath(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return null;
        }
        try {
            return Paths.get(rawPath).toAbsolutePath().normalize();
        } catch (Exception error) {
            return null;
        }
    }

    private boolean containsReusableMarkdown(Path candidate) {
        if (candidate == null || !Files.exists(candidate)) {
            return false;
        }
        if (Files.isRegularFile(candidate)) {
            return isMarkdownFile(candidate);
        }
        if (!Files.isDirectory(candidate)) {
            return false;
        }
        try (Stream<Path> stream = Files.walk(candidate, REUSABLE_MARKDOWN_SCAN_DEPTH)) {
            return stream
                    .filter(Files::isRegularFile)
                    .anyMatch(this::isMarkdownFile);
        } catch (IOException error) {
            return false;
        }
    }

    private boolean isMarkdownFile(Path path) {
        if (path == null || path.getFileName() == null) {
            return false;
        }
        String fileName = path.getFileName().toString().trim().toLowerCase(Locale.ROOT);
        return fileName.endsWith(".md");
    }

    public static class NormalizedTaskInput {
        public final String rawInput;
        public final String normalizedVideoUrl;
        public final String normalizedVideoKey;

        public NormalizedTaskInput(String rawInput, String normalizedVideoUrl, String normalizedVideoKey) {
            this.rawInput = rawInput != null ? rawInput : "";
            this.normalizedVideoUrl = normalizedVideoUrl != null ? normalizedVideoUrl : "";
            this.normalizedVideoKey = normalizedVideoKey != null ? normalizedVideoKey : "";
        }
    }
}
