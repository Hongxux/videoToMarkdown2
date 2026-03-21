package com.mvp.module2.fusion.service;

import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class TaskProbeService {

    @Autowired
    private PythonGrpcClient pythonGrpcClient;

    @Autowired(required = false)
    private Phase2bArticleLinkService phase2bArticleLinkService;

    @Value("${grpc.python.timeout-seconds:300}")
    private int grpcTimeoutSeconds;

    public ProbeOutcome probeTask(TaskQueueManager.TaskEntry task) {
        if (task == null) {
            return ProbeOutcome.failure("task is null");
        }
        String videoUrl = normalize(task.videoUrl);
        if (videoUrl.isEmpty()) {
            return ProbeOutcome.failure("task videoUrl is blank");
        }
        if (looksLikeBookTask(task)) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contentType", "book");
            payload.put("resolvedUrl", videoUrl);
            payload.put("title", resolveBookTitle(task, videoUrl));
            payload.put("probeMode", "book-shortcut");
            return ProbeOutcome.success(
                    resolveBookTitle(task, videoUrl),
                    "书籍任务探测完成，开始进入处理链路",
                    payload
            );
        }
        if (looksLikeLocalFile(videoUrl)) {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contentType", "local_file");
            payload.put("resolvedUrl", videoUrl);
            payload.put("title", resolveLocalFileName(videoUrl));
            payload.put("probeMode", "local-file-shortcut");
            return ProbeOutcome.success(
                    resolveLocalFileName(videoUrl),
                    "本地文件探测完成，开始进入处理链路",
                    payload
            );
        }
        if (phase2bArticleLinkService != null) {
            try {
                List<String> normalizedLinks = phase2bArticleLinkService.normalizeSupportedLinks(List.of(videoUrl));
                if (!normalizedLinks.isEmpty()) {
                    String resolvedUrl = normalize(normalizedLinks.get(0));
                    Phase2bArticleLinkService.LinkMetadata metadata = null;
                    List<Phase2bArticleLinkService.LinkMetadata> metadataList =
                            phase2bArticleLinkService.prefetchLinkMetadata(List.of(resolvedUrl));
                    if (!metadataList.isEmpty()) {
                        metadata = metadataList.get(0);
                    }
                    Map<String, Object> payload = new LinkedHashMap<>();
                    payload.put("contentType", "article");
                    payload.put("resolvedUrl", resolvedUrl);
                    payload.put("sourcePlatform", metadata != null ? normalize(metadata.siteType) : "");
                    payload.put("title", metadata != null ? normalize(metadata.title) : "");
                    payload.put("probeMode", "article-link");
                    return ProbeOutcome.success(
                            metadata != null ? normalize(metadata.title) : "",
                            "文章链接探测完成，开始进入处理链路",
                            payload
                    );
                }
            } catch (Exception ignored) {
                // 文章链接探测失败时回落到通用视频探测。
            }
        }
        PythonGrpcClient.VideoInfoResult result = pythonGrpcClient.getVideoInfo(
                task.taskId,
                videoUrl,
                Math.max(30, grpcTimeoutSeconds)
        );
        if (result == null || !result.success) {
            String error = result != null ? normalize(result.errorMsg) : "";
            if (error.isEmpty()) {
                error = "probe returned unsuccessful result";
            }
            return ProbeOutcome.failure(error);
        }
        String preferredTitle = formatEpisodeAwareTitle(
                result.isCollection,
                normalize(result.videoTitle),
                result.currentEpisodeIndex,
                normalize(result.currentEpisodeTitle)
        );
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("rawInput", normalize(result.rawInput));
        payload.put("resolvedUrl", normalize(result.resolvedUrl));
        payload.put("sourcePlatform", normalize(result.sourcePlatform));
        payload.put("canonicalId", normalize(result.canonicalId));
        payload.put("title", preferredTitle);
        payload.put("durationSec", result.durationSec);
        payload.put("isCollection", result.isCollection);
        payload.put("totalEpisodes", result.totalEpisodes);
        payload.put("currentEpisodeIndex", result.currentEpisodeIndex);
        payload.put("currentEpisodeTitle", normalize(result.currentEpisodeTitle));
        payload.put("linkResolver", normalize(result.linkResolver));
        payload.put("contentType", normalize(result.contentType));
        payload.put("coverUrl", normalize(result.coverUrl));
        payload.put("episodes", result.episodes != null ? result.episodes : List.of());
        payload.put("probeMode", "python-video-info");
        return ProbeOutcome.success(
                preferredTitle,
                "任务探测完成，开始进入处理链路",
                payload
        );
    }

    private boolean looksLikeBookTask(TaskQueueManager.TaskEntry task) {
        if (task == null) {
            return false;
        }
        String lower = normalize(task.videoUrl).toLowerCase(Locale.ROOT);
        if (lower.endsWith(".pdf") || lower.endsWith(".epub") || lower.endsWith(".txt") || lower.endsWith(".md")) {
            return true;
        }
        return task.bookOptions != null;
    }

    private boolean looksLikeLocalFile(String value) {
        String normalized = normalize(value);
        if (normalized.isEmpty()) {
            return false;
        }
        String lower = normalized.toLowerCase(Locale.ROOT);
        if (lower.startsWith("http://") || lower.startsWith("https://")) {
            return false;
        }
        try {
            Paths.get(normalized);
            return true;
        } catch (Exception error) {
            return false;
        }
    }

    private String resolveBookTitle(TaskQueueManager.TaskEntry task, String videoUrl) {
        if (task != null && task.bookOptions != null) {
            String title = normalize(task.bookOptions.leafTitle);
            if (!title.isEmpty()) {
                return title;
            }
            title = normalize(task.bookOptions.bookTitle);
            if (!title.isEmpty()) {
                return title;
            }
        }
        return resolveLocalFileName(videoUrl);
    }

    private String resolveLocalFileName(String videoUrl) {
        String normalized = normalize(videoUrl);
        if (normalized.isEmpty()) {
            return "";
        }
        try {
            Path path = Paths.get(normalized);
            Path fileName = path.getFileName();
            return fileName != null ? normalize(fileName.toString()) : normalized;
        } catch (Exception error) {
            return normalized;
        }
    }

    public static String formatVideoInfoTitle(PythonGrpcClient.VideoInfoResult result) {
        if (result == null) {
            return "";
        }
        return formatEpisodeAwareTitle(
                result.isCollection,
                normalizeText(result.videoTitle),
                result.currentEpisodeIndex,
                normalizeText(result.currentEpisodeTitle)
        );
    }

    public static String formatProbePayloadTitle(Map<String, Object> payload) {
        if (payload == null || payload.isEmpty()) {
            return "";
        }
        return formatEpisodeAwareTitle(
                readProbeBoolean(payload.get("isCollection")),
                normalizeText(String.valueOf(payload.getOrDefault("title", ""))),
                readProbeInt(payload.get("currentEpisodeIndex")),
                normalizeText(String.valueOf(payload.getOrDefault("currentEpisodeTitle", "")))
        );
    }

    public static String formatEpisodeAwareTitle(
            boolean isCollection,
            String baseTitle,
            int currentEpisodeIndex,
            String currentEpisodeTitle
    ) {
        String normalizedBaseTitle = normalizeText(baseTitle);
        String normalizedEpisodeTitle = normalizeText(currentEpisodeTitle);
        if (!isCollection || currentEpisodeIndex <= 0 || normalizedEpisodeTitle.isEmpty()) {
            return normalizedBaseTitle;
        }
        if (normalizedBaseTitle.isEmpty()) {
            return normalizedEpisodeTitle;
        }
        String suffix = "_p" + currentEpisodeIndex + "_" + normalizedEpisodeTitle;
        if (normalizedBaseTitle.endsWith(suffix)) {
            return normalizedBaseTitle;
        }
        return normalizedBaseTitle + suffix;
    }

    private String normalize(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    private static String normalizeText(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value.trim();
        return "null".equalsIgnoreCase(normalized) ? "" : normalized;
    }

    private static boolean readProbeBoolean(Object value) {
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        if (value == null) {
            return false;
        }
        return "true".equalsIgnoreCase(String.valueOf(value).trim());
    }

    private static int readProbeInt(Object value) {
        if (value instanceof Number numberValue) {
            return numberValue.intValue();
        }
        if (value == null) {
            return 0;
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (Exception ignored) {
            return 0;
        }
    }

    public static class ProbeOutcome {
        public final boolean success;
        public final String preferredTitle;
        public final String statusMessage;
        public final Map<String, Object> payload;
        public final String errorMessage;

        private ProbeOutcome(
                boolean success,
                String preferredTitle,
                String statusMessage,
                Map<String, Object> payload,
                String errorMessage
        ) {
            this.success = success;
            this.preferredTitle = preferredTitle;
            this.statusMessage = statusMessage;
            this.payload = payload;
            this.errorMessage = errorMessage;
        }

        public static ProbeOutcome success(String preferredTitle, String statusMessage, Map<String, Object> payload) {
            return new ProbeOutcome(true, preferredTitle, statusMessage, payload, "");
        }

        public static ProbeOutcome failure(String errorMessage) {
            return new ProbeOutcome(false, "", "", Map.of(), errorMessage == null ? "" : errorMessage.trim());
        }
    }
}
