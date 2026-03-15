package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class TaskRuntimeRecoveryService {

    private static final Logger logger = LoggerFactory.getLogger(TaskRuntimeRecoveryService.class);
    private static final Pattern BILIBILI_BV_PATTERN =
            Pattern.compile("BV[0-9A-Za-z]{10}", Pattern.CASE_INSENSITIVE);
    private static final Pattern BILIBILI_AV_PATTERN =
            Pattern.compile("(?:^|[^0-9A-Za-z])av(\\d{1,20})(?:$|[^0-9A-Za-z])", Pattern.CASE_INSENSITIVE);
    private static final Set<String> BLOCKING_STAGE_STATUSES =
            Set.of("MANUAL_RETRY_REQUIRED", "FATAL");

    private final ObjectMapper objectMapper;

    @Value("${task.storage.root:}")
    private String configuredStorageRoot;

    public TaskRuntimeRecoveryService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Optional<RecoveryDirective> resolveBlockingDirective(TaskQueueManager.TaskEntry task) {
        if (task == null) {
            return Optional.empty();
        }
        return resolveBlockingDirective(task.videoUrl, task.outputDir, task.resultPath);
    }

    public Optional<RecoveryDirective> resolveBlockingDirective(
            String videoUrl,
            String outputDir,
            String resultPath
    ) {
        Optional<RecoveryDirective> latest = resolveLatestStageState(videoUrl, outputDir, resultPath);
        if (latest.isEmpty()) {
            return Optional.empty();
        }
        RecoveryDirective directive = latest.get();
        if (!BLOCKING_STAGE_STATUSES.contains(directive.stageStatus())) {
            return Optional.empty();
        }
        return Optional.of(directive);
    }

    public Optional<RecoveryDirective> resolveLatestStageState(
            String videoUrl,
            String outputDir,
            String resultPath
    ) {
        RecoveryDirective latest = null;
        for (Path candidateDir : resolveCandidateTaskDirs(videoUrl, outputDir, resultPath)) {
            RecoveryDirective candidate = loadLatestDirectiveFromTaskDir(candidateDir);
            if (candidate == null) {
                continue;
            }
            if (latest == null || candidate.updatedAtMs() > latest.updatedAtMs()) {
                latest = candidate;
            }
        }
        return Optional.ofNullable(latest);
    }

    private List<Path> resolveCandidateTaskDirs(String videoUrl, String outputDir, String resultPath) {
        LinkedHashSet<Path> candidates = new LinkedHashSet<>();
        addCandidateDir(candidates, outputDir);
        addCandidateDir(candidates, resolveTaskRootFromResultPath(resultPath));

        Path storageRoot = resolveStorageRoot();
        if (storageRoot != null) {
            String normalizedInput = firstNonBlank(videoUrl, "");
            if (!normalizedInput.isBlank()) {
                if (isHttpUrl(normalizedInput)) {
                    candidates.add(storageRoot.resolve(md5Hex(buildDownloadTaskDirSource(normalizedInput))));
                } else {
                    candidates.add(storageRoot.resolve(md5Hex(normalizePathForHash(normalizedInput))));
                }
            }
        }
        return new ArrayList<>(candidates);
    }

    private void addCandidateDir(LinkedHashSet<Path> candidates, String rawPath) {
        String normalized = trimToNull(rawPath);
        if (normalized == null) {
            return;
        }
        try {
            candidates.add(Paths.get(normalized).toAbsolutePath().normalize());
        } catch (Exception ignored) {
            // 这里是恢复兜底路径，不让单个坏路径中断整体恢复。
        }
    }

    private String resolveTaskRootFromResultPath(String resultPath) {
        String normalized = trimToNull(resultPath);
        if (normalized == null) {
            return null;
        }
        try {
            Path path = Paths.get(normalized).toAbsolutePath().normalize();
            if (Files.isDirectory(path)) {
                return path.toString();
            }
            Path parent = path.getParent();
            return parent != null ? parent.toString() : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    private RecoveryDirective loadLatestDirectiveFromTaskDir(Path taskDir) {
        if (taskDir == null) {
            return null;
        }
        Path stageRoot = taskDir.resolve("intermediates").resolve("rt").resolve("s");
        if (!Files.isDirectory(stageRoot)) {
            return null;
        }
        RecoveryDirective latest = null;
        try (var stageDirs = Files.list(stageRoot)) {
            for (Path stageDir : (Iterable<Path>) stageDirs::iterator) {
                Path stageStatePath = stageDir.resolve("stage_state.json");
                if (!Files.isRegularFile(stageStatePath)) {
                    continue;
                }
                RecoveryDirective candidate = parseDirective(stageStatePath);
                if (candidate == null) {
                    continue;
                }
                if (latest == null || candidate.updatedAtMs() > latest.updatedAtMs()) {
                    latest = candidate;
                }
            }
        } catch (IOException ioError) {
            logger.debug("Resolve runtime stage state skipped: taskDir={} err={}", taskDir, ioError.getMessage());
        }
        return latest;
    }

    private RecoveryDirective parseDirective(Path stageStatePath) {
        try {
            JsonNode root = objectMapper.readTree(Files.readAllBytes(stageStatePath));
            if (root == null || !root.isObject()) {
                return null;
            }
            long updatedAtMs = readLong(root, "updated_at_ms");
            if (updatedAtMs <= 0L) {
                updatedAtMs = Files.getLastModifiedTime(stageStatePath).toMillis();
            }
            String stage = firstNonBlank(readText(root, "stage"), stageStatePath.getParent().getFileName().toString());
            String stageStatus = firstNonBlank(readText(root, "status"), "UNKNOWN");
            String checkpoint = firstNonBlank(readText(root, "checkpoint"), "unknown");
            String retryMode = firstNonBlank(readText(root, "retry_mode"), "");
            String requiredAction = firstNonBlank(readText(root, "required_action"), "");
            String retryEntryPoint = firstNonBlank(readText(root, "retry_entry_point"), "");
            String retryStrategy = firstNonBlank(readText(root, "retry_strategy"), "");
            String operatorAction = firstNonBlank(readText(root, "operator_action"), "");
            String actionHint = firstNonBlank(readText(root, "action_hint"), "");
            String errorClass = firstNonBlank(readText(root, "error_class"), "");
            String errorMessage = firstNonBlank(readText(root, "error_message"), "");
            String outputDir = firstNonBlank(readText(root, "output_dir"), "");
            return new RecoveryDirective(
                    stage,
                    stageStatus,
                    checkpoint,
                    updatedAtMs,
                    retryMode,
                    requiredAction,
                    retryEntryPoint,
                    retryStrategy,
                    operatorAction,
                    actionHint,
                    errorClass,
                    errorMessage,
                    outputDir,
                    stageStatePath.toString()
            );
        } catch (Exception parseError) {
            logger.debug("Parse runtime stage state skipped: path={} err={}", stageStatePath, parseError.getMessage());
            return null;
        }
    }

    private Path resolveStorageRoot() {
        String configured = trimToNull(configuredStorageRoot);
        if (configured != null) {
            try {
                return Paths.get(configured).toAbsolutePath().normalize();
            } catch (Exception ignored) {
                // 配置非法时继续回退到环境变量或默认目录。
            }
        }
        String envRoot = trimToNull(System.getenv("V2M_STORAGE_ROOT"));
        if (envRoot != null) {
            try {
                return Paths.get(envRoot).toAbsolutePath().normalize();
            } catch (Exception ignored) {
                // 环境变量非法时继续回退到仓库默认目录。
            }
        }
        return Paths.get("var", "storage", "storage").toAbsolutePath().normalize();
    }

    private boolean isHttpUrl(String value) {
        if (value == null || value.isBlank()) {
            return false;
        }
        try {
            URI parsed = URI.create(value);
            String scheme = parsed.getScheme();
            if (scheme == null) {
                return false;
            }
            String normalized = scheme.toLowerCase(Locale.ROOT);
            return "http".equals(normalized) || "https".equals(normalized);
        } catch (Exception ignored) {
            return false;
        }
    }

    private String buildDownloadTaskDirSource(String videoUrl) {
        String bilibiliVideoId = extractBilibiliVideoId(videoUrl);
        if (bilibiliVideoId != null && !bilibiliVideoId.isBlank()) {
            Integer bilibiliEpisodeIndex = extractBilibiliEpisodeIndex(videoUrl);
            if (bilibiliEpisodeIndex != null && bilibiliEpisodeIndex > 0) {
                return bilibiliVideoId + "_" + bilibiliEpisodeIndex;
            }
            return bilibiliVideoId;
        }
        return videoUrl == null ? "" : videoUrl;
    }

    private String extractBilibiliVideoId(String videoUrl) {
        if (videoUrl == null || videoUrl.isBlank()) {
            return null;
        }
        try {
            URI parsed = URI.create(videoUrl);
            if (!isBilibiliHost(parsed.getHost())) {
                return null;
            }
            Map<String, String> query = parseQueryParams(parsed.getRawQuery());
            String bvid = query.getOrDefault("bvid", "");
            if (!bvid.isBlank()) {
                Matcher bvidMatcher = BILIBILI_BV_PATTERN.matcher(bvid);
                if (bvidMatcher.find()) {
                    return bvidMatcher.group();
                }
            }
            String aid = query.getOrDefault("aid", "");
            if (!aid.isBlank() && aid.chars().allMatch(Character::isDigit)) {
                return "AV" + aid;
            }
            String searchSpace = String.join(
                    " ",
                    firstNonBlank(parsed.getRawPath(), ""),
                    firstNonBlank(parsed.getRawQuery(), ""),
                    firstNonBlank(parsed.getRawFragment(), "")
            );
            Matcher bvMatcher = BILIBILI_BV_PATTERN.matcher(searchSpace);
            if (bvMatcher.find()) {
                return bvMatcher.group();
            }
            Matcher avMatcher = BILIBILI_AV_PATTERN.matcher(searchSpace);
            if (avMatcher.find()) {
                return "AV" + avMatcher.group(1);
            }
            return null;
        } catch (Exception ignored) {
            return null;
        }
    }

    private Integer extractBilibiliEpisodeIndex(String videoUrl) {
        if (videoUrl == null || videoUrl.isBlank()) {
            return null;
        }
        try {
            URI parsed = URI.create(videoUrl);
            if (!isBilibiliHost(parsed.getHost())) {
                return null;
            }
            Map<String, String> query = parseQueryParams(parsed.getRawQuery());
            String rawEpisode = firstNonBlank(query.get("p"), query.get("P"), "");
            if (rawEpisode.isBlank()) {
                return null;
            }
            int episodeIndex = Integer.parseInt(rawEpisode);
            return episodeIndex > 0 ? episodeIndex : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    private boolean isBilibiliHost(String host) {
        if (host == null || host.isBlank()) {
            return false;
        }
        String normalized = host.toLowerCase(Locale.ROOT).split(":", 2)[0];
        return normalized.equals("bilibili.com")
                || normalized.endsWith(".bilibili.com")
                || normalized.equals("b23.tv")
                || normalized.endsWith(".b23.tv");
    }

    private Map<String, String> parseQueryParams(String rawQuery) {
        Map<String, String> params = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) {
            return params;
        }
        for (String pair : rawQuery.split("&")) {
            if (pair == null || pair.isBlank()) {
                continue;
            }
            String[] tokens = pair.split("=", 2);
            String key = decodeUrlComponent(tokens[0]);
            String value = tokens.length > 1 ? decodeUrlComponent(tokens[1]) : "";
            if (!key.isBlank() && !params.containsKey(key)) {
                params.put(key, value);
            }
        }
        return params;
    }

    private String decodeUrlComponent(String value) {
        if (value == null) {
            return "";
        }
        try {
            return java.net.URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (Exception ignored) {
            return value;
        }
    }

    private String normalizePathForHash(String path) {
        String abs = new File(path).getAbsolutePath();
        String normalized = abs.replace('/', File.separatorChar);
        if (File.separatorChar == '\\') {
            normalized = normalized.toLowerCase(Locale.ROOT);
        }
        return normalized;
    }

    private String md5Hex(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception error) {
            throw new IllegalStateException("md5 hash failed", error);
        }
    }

    private String readText(JsonNode node, String fieldName) {
        if (node == null || fieldName == null || fieldName.isBlank()) {
            return "";
        }
        JsonNode child = node.get(fieldName);
        if (child == null || child.isNull()) {
            return "";
        }
        if (child.isTextual()) {
            return child.asText("");
        }
        return child.toString();
    }

    private long readLong(JsonNode node, String fieldName) {
        if (node == null || fieldName == null || fieldName.isBlank()) {
            return 0L;
        }
        JsonNode child = node.get(fieldName);
        if (child == null || child.isNull()) {
            return 0L;
        }
        if (child.isNumber()) {
            return child.asLong(0L);
        }
        try {
            return Long.parseLong(child.asText("0").trim());
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            String normalized = trimToNull(value);
            if (normalized != null) {
                return normalized;
            }
        }
        return "";
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    public record RecoveryDirective(
            String stage,
            String stageStatus,
            String checkpoint,
            long updatedAtMs,
            String retryMode,
            String requiredAction,
            String retryEntryPoint,
            String retryStrategy,
            String operatorAction,
            String actionHint,
            String errorClass,
            String errorMessage,
            String outputDir,
            String stageStatePath
    ) {
        public Map<String, Object> toPayload() {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("stage", firstNonBlank(stage, ""));
            payload.put("stageStatus", firstNonBlank(stageStatus, ""));
            payload.put("checkpoint", firstNonBlank(checkpoint, ""));
            payload.put("updatedAtMs", Math.max(0L, updatedAtMs));
            payload.put("retryMode", firstNonBlank(retryMode, ""));
            payload.put("requiredAction", firstNonBlank(requiredAction, ""));
            payload.put("retryEntryPoint", firstNonBlank(retryEntryPoint, ""));
            payload.put("retryStrategy", firstNonBlank(retryStrategy, ""));
            payload.put("operatorAction", firstNonBlank(operatorAction, ""));
            payload.put("actionHint", firstNonBlank(actionHint, ""));
            payload.put("errorClass", firstNonBlank(errorClass, ""));
            payload.put("errorMessage", firstNonBlank(errorMessage, ""));
            payload.put("outputDir", firstNonBlank(outputDir, ""));
            payload.put("stageStatePath", firstNonBlank(stageStatePath, ""));
            return payload;
        }

        public String buildStatusMessage() {
            String stageLabel = firstNonBlank(stage, "unknown_stage");
            String checkpointLabel = firstNonBlank(checkpoint, "unknown");
            String action = firstNonBlank(requiredAction, actionHint, errorMessage, "需要人工检查后重试");
            return String.format("[%s/%s] %s", stageLabel, checkpointLabel, action);
        }

        public static String firstNonBlank(String... values) {
            if (values == null) {
                return "";
            }
            for (String value : values) {
                if (value == null) {
                    continue;
                }
                String trimmed = value.trim();
                if (!trimmed.isEmpty()) {
                    return trimmed;
                }
            }
            return "";
        }
    }
}
