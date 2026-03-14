package com.mvp.module2.fusion.service;

import com.mvp.module2.fusion.common.VideoInputNormalizer;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TaskDeduplicationService {

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
        return taskStateRepository.findLatestReusableTaskByNormalizedVideoKey(normalizedVideoKey, excludeTaskId);
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
