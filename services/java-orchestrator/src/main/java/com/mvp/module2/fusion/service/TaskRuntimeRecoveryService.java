package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.queue.TaskQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
    private static final Set<String> BLOCKING_STAGE_STATUSES = Set.of("MANUAL_NEEDED", "FAILED");
    private static final String FALLBACK_AUDIT_FILE_NAME = "fallback_records.jsonl";
    private static final String LEGACY_FALLBACK_AUDIT_FILE_NAME = "llm_fallback_events.jsonl";
    private static final List<String> VIDEO_STAGE_GRAPH = List.of(
            "download", "transcribe", "stage1", "phase2a", "asset_extract_java", "phase2b"
    );

    private final ObjectMapper objectMapper;
    private final TaskRuntimeStageStore taskRuntimeStageStore;

    @Value("${task.storage.root:}")
    private String configuredStorageRoot;

    public TaskRuntimeRecoveryService(ObjectMapper objectMapper, TaskRuntimeStageStore taskRuntimeStageStore) {
        this.objectMapper = objectMapper;
        this.taskRuntimeStageStore = taskRuntimeStageStore;
    }

    public Optional<RecoveryDirective> resolveBlockingDirective(TaskQueueManager.TaskEntry task) {
        if (task == null) {
            return Optional.empty();
        }
        return resolveBlockingDirective(task.videoUrl, task.outputDir, task.resultPath);
    }

    public Optional<RecoveryDirective> resolveBlockingDirective(String videoUrl, String outputDir, String resultPath) {
        Optional<RecoveryDirective> latest = resolveLatestStageState(videoUrl, outputDir, resultPath);
        if (latest.isEmpty()) {
            return Optional.empty();
        }
        RecoveryDirective directive = latest.get();
        return BLOCKING_STAGE_STATUSES.contains(normalizeStatus(directive.stageStatus()))
                ? Optional.of(directive)
                : Optional.empty();
    }

    public Optional<RecoveryDirective> resolveLatestStageState(String videoUrl, String outputDir, String resultPath) {
        StageSnapshot latest = null;
        for (Path taskDir : resolveCandidateTaskDirs(videoUrl, outputDir, resultPath)) {
            Map<String, StageSnapshot> snapshots = scanStageSnapshots(taskDir);
            StageSnapshot candidate = latestSnapshot(snapshots.values());
            if (candidate != null && (latest == null || candidate.updatedAtMs() > latest.updatedAtMs())) {
                latest = candidate;
            }
        }
        return Optional.ofNullable(latest).map(StageSnapshot::toRecoveryDirective);
    }

    public Optional<ResumeDecision> resolveResumeDecision(TaskQueueManager.TaskEntry task) {
        if (task == null) {
            return Optional.empty();
        }
        return resolveResumeDecision(task.videoUrl, task.outputDir, task.resultPath);
    }

    public Optional<ResumeDecision> resolveResumeDecision(String videoUrl, String outputDir, String resultPath) {
        ResumeDecision best = null;
        for (Path taskDir : resolveCandidateTaskDirs(videoUrl, outputDir, resultPath)) {
            ResumeDecision candidate = resolveResumeDecisionFromTaskDir(taskDir);
            if (candidate == null) {
                continue;
            }
            if (best == null) {
                best = candidate;
                continue;
            }
            long candidateUpdatedAt = candidate.latestStageSnapshot() != null ? candidate.latestStageSnapshot().updatedAtMs() : 0L;
            long bestUpdatedAt = best.latestStageSnapshot() != null ? best.latestStageSnapshot().updatedAtMs() : 0L;
            if (candidateUpdatedAt > bestUpdatedAt) {
                best = candidate;
            }
        }
        return Optional.ofNullable(best);
    }

    public Optional<FallbackRepairDirective> resolveFallbackRepairDirective(TaskQueueManager.TaskEntry task) {
        if (task == null || task.status == null) {
            return Optional.empty();
        }
        if (!BLOCKING_STAGE_STATUSES.contains(normalizeStatus(task.status.name()))) {
            return Optional.empty();
        }
        FallbackRepairDirective best = null;
        for (Path taskDir : resolveCandidateTaskDirs(task.videoUrl, task.outputDir, task.resultPath)) {
            FallbackRepairDirective candidate = loadFallbackRepairDirective(taskDir);
            if (candidate == null) {
                continue;
            }
            if (best == null || candidate.updatedAtMs() > best.updatedAtMs()) {
                best = candidate;
            }
        }
        return Optional.ofNullable(best);
    }

    public Optional<FallbackRepairDirective> prepareFallbackRepair(TaskQueueManager.TaskEntry task) {
        if (task == null || task.taskId == null || task.taskId.isBlank() || taskRuntimeStageStore == null) {
            return Optional.empty();
        }
        Optional<FallbackRepairDirective> directiveOpt = resolveFallbackRepairDirective(task);
        if (directiveOpt.isEmpty()) {
            return Optional.empty();
        }
        FallbackRepairDirective directive = directiveOpt.get();
        String outputDir = firstNonBlank(task.outputDir, directive.taskDir().toString());
        Map<String, Object> dirtyPlan = taskRuntimeStageStore.markScopesDirty(
                outputDir,
                directive.scopeRefs(),
                directive.reason(),
                true
        );
        Map<String, Object> extraPayload = new LinkedHashMap<>(directive.toPayload());
        extraPayload.put("retryMode", "manual");
        extraPayload.put("requiredAction", "请先修复 fallback 根因后再执行定点恢复");
        extraPayload.put("retryEntryPoint", "fallback_repair:" + directive.stage());
        extraPayload.put("dirtyScopeRefs", dirtyPlan.getOrDefault("dirty_scope_refs", List.of()));
        extraPayload.put("dirtyScopeCount", dirtyPlan.getOrDefault("dirty_scope_count", directive.scopeRefs().size()));
        Path stageStatePath = taskRuntimeStageStore.writeStageState(
                outputDir,
                task.taskId,
                directive.stage(),
                "MANUAL_NEEDED",
                "fallback_repair_requested",
                0,
                Math.max(1, directive.scopeRefs().size()),
                extraPayload
        );
        taskRuntimeStageStore.appendStageJournalEvent(
                outputDir,
                task.taskId,
                directive.stage(),
                "fallback_repair_requested",
                "MANUAL_NEEDED",
                "fallback_repair_requested",
                0,
                Math.max(1, directive.scopeRefs().size()),
                extraPayload
        );
        taskRuntimeStageStore.writeResumeIndexHint(
                outputDir,
                task.taskId,
                directive.stage(),
                "MANUAL_NEEDED",
                "fallback_repair_requested",
                stageStatePath,
                "fallback_repair_requested"
        );
        return Optional.of(directive);
    }

    private ResumeDecision resolveResumeDecisionFromTaskDir(Path taskDir) {
        if (taskDir == null || !Files.isDirectory(taskDir)) {
            return null;
        }
        Map<String, StageSnapshot> snapshots = scanStageSnapshots(taskDir);
        StageSnapshot latest = latestSnapshot(snapshots.values());
        StageSnapshot hinted = loadHintedSnapshot(taskDir, snapshots);
        StageSnapshot anchor = hinted != null ? hinted : latest;
        ResumeProbe probe = resolveResumeProbe(snapshots, anchor);
        if (probe == null) {
            return null;
        }
        return new ResumeDecision(
                taskDir,
                probe.resumeFromStage(),
                probe.stageOwner(),
                probe.resumeSnapshot(),
                hinted,
                latest,
                Collections.unmodifiableMap(new LinkedHashMap<>(snapshots)),
                probe.reason()
        );
    }

    private Map<String, StageSnapshot> scanStageSnapshots(Path taskDir) {
        Map<String, StageSnapshot> snapshots = new LinkedHashMap<>();
        if (taskRuntimeStageStore != null && taskDir != null) {
            try {
                Map<String, TaskRuntimeStageStore.StageSnapshotRecord> dbSnapshots =
                        taskRuntimeStageStore.loadStageSnapshots(taskDir.toString());
                for (TaskRuntimeStageStore.StageSnapshotRecord record : dbSnapshots.values()) {
                    StageSnapshot snapshot = toStageSnapshot(record, taskDir);
                    if (snapshot == null) {
                        continue;
                    }
                    StageSnapshot previous = snapshots.get(snapshot.stage());
                    if (previous == null || snapshot.updatedAtMs() >= previous.updatedAtMs()) {
                        snapshots.put(snapshot.stage(), snapshot);
                    }
                }
            } catch (Exception error) {
                logger.debug("Resolve runtime stage states from sqlite skipped: taskDir={} err={}", taskDir, error.getMessage());
            }
        }
        for (Path stageRoot : List.of(
                taskDir.resolve("intermediates").resolve("rt").resolve("stage"),
                taskDir.resolve("intermediates").resolve("rt").resolve("stages"),
                taskDir.resolve("intermediates").resolve("rt").resolve("s")
        )) {
            if (!Files.isDirectory(stageRoot)) {
                continue;
            }
            try (var stageDirs = Files.list(stageRoot)) {
                for (Path stageDir : (Iterable<Path>) stageDirs::iterator) {
                    Path stageStatePath = stageDir.resolve("stage_state.json");
                    if (!Files.isRegularFile(stageStatePath)) {
                        continue;
                    }
                    StageSnapshot snapshot = parseStageSnapshot(stageStatePath);
                    if (snapshot == null) {
                        continue;
                    }
                    StageSnapshot previous = snapshots.get(snapshot.stage());
                    if (previous == null || snapshot.updatedAtMs() >= previous.updatedAtMs()) {
                        snapshots.put(snapshot.stage(), snapshot);
                    }
                }
            } catch (Exception error) {
                logger.debug("Resolve runtime stage states skipped: taskDir={} root={} err={}", taskDir, stageRoot, error.getMessage());
            }
        }
        return snapshots;
    }

    private StageSnapshot toStageSnapshot(TaskRuntimeStageStore.StageSnapshotRecord record, Path taskDir) {
        if (record == null) {
            return null;
        }
        Map<String, Object> payload = new LinkedHashMap<>(record.payload() != null ? record.payload() : Map.of());
        String stageStatePath = firstNonBlank(
                record.stageStatePath(),
                taskDir != null ? taskDir.resolve("intermediates").resolve("rt").resolve("stage")
                        .resolve(normalizeStage(record.stage())).resolve("stage_state.json").toString() : ""
        );
        String outputDir = firstNonBlank(record.outputDir(), taskDir != null ? taskDir.toString() : "");
        payload.putIfAbsent("stage_state_path", stageStatePath);
        payload.putIfAbsent("local_stage_state_path", stageStatePath);
        payload.putIfAbsent("output_dir", outputDir);
        return new StageSnapshot(
                normalizeStage(record.stage()),
                firstNonBlank(record.stageOwner(), ownerOf(record.stage())),
                firstNonBlank(record.status(), "UNKNOWN"),
                firstNonBlank(record.checkpoint(), "unknown"),
                Math.max(0L, record.updatedAtMs()),
                outputDir,
                stageStatePath,
                Collections.unmodifiableMap(payload)
        );
    }

    private StageSnapshot loadHintedSnapshot(Path taskDir, Map<String, StageSnapshot> snapshots) {
        Path resumeIndexPath = taskDir.resolve("intermediates").resolve("rt").resolve("resume_index.json");
        if (!Files.isRegularFile(resumeIndexPath)) {
            return null;
        }
        try {
            JsonNode root = objectMapper.readTree(Files.readAllBytes(resumeIndexPath));
            if (root == null || !root.isObject()) {
                return null;
            }
            for (HintCandidate hint : readHints(root)) {
                StageSnapshot exact = parseSnapshotAtHintPath(taskDir, hint.stageStatePath());
                if (exact != null) {
                    return exact;
                }
                StageSnapshot hintedByPath = findSnapshotByStageStatePath(taskDir, snapshots, hint.stageStatePath());
                if (hintedByPath != null) {
                    return hintedByPath;
                }
                if (!hint.stage().isBlank() && snapshots.containsKey(hint.stage())) {
                    return snapshots.get(hint.stage());
                }
            }
        } catch (Exception error) {
            logger.debug("Parse runtime resume index skipped: path={} err={}", resumeIndexPath, error.getMessage());
        }
        return null;
    }

    private List<HintCandidate> readHints(JsonNode root) {
        List<HintCandidate> hints = new ArrayList<>();
        String hintStage = normalizeStage(firstNonBlank(readText(root, "hint_stage"), readText(root, "stage"), ""));
        String hintPath = firstNonBlank(readText(root, "hint_stage_state_path"), readText(root, "stage_state_path"), "");
        if (!hintStage.isBlank() || !hintPath.isBlank()) {
            hints.add(new HintCandidate(hintStage, hintPath));
        }
        appendLegacyHint(hints, root.get("latest_blocking_stage_state"));
        appendLegacyHint(hints, root.get("previous_blocking_stage_state"));
        appendLegacyHint(hints, root.get("latest_stage_state"));
        appendLegacyHint(hints, root.get("previous_stage_state"));
        return hints;
    }

    private void appendLegacyHint(List<HintCandidate> hints, JsonNode summaryNode) {
        if (summaryNode == null || !summaryNode.isObject()) {
            return;
        }
        hints.add(new HintCandidate(
                normalizeStage(readText(summaryNode, "stage")),
                firstNonBlank(readText(summaryNode, "stage_state_path"), readText(summaryNode, "local_stage_state_path"), "")
        ));
    }

    private StageSnapshot parseSnapshotAtHintPath(Path taskDir, String rawStageStatePath) {
        String normalized = trimToNull(rawStageStatePath);
        if (normalized == null) {
            return null;
        }
        try {
            Path stageStatePath = Paths.get(normalized);
            if (!stageStatePath.isAbsolute()) {
                stageStatePath = taskDir.resolve(stageStatePath);
            }
            stageStatePath = stageStatePath.toAbsolutePath().normalize();
            return Files.isRegularFile(stageStatePath) ? parseStageSnapshot(stageStatePath) : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    private ResumeProbe resolveResumeProbe(Map<String, StageSnapshot> snapshots, StageSnapshot anchor) {
        if (anchor != null && VIDEO_STAGE_GRAPH.contains(anchor.stage())) {
            if (!isCompletedStatus(anchor.status())) {
                return new ResumeProbe(anchor.stage(), firstNonBlank(anchor.stageOwner(), ownerOf(anchor.stage())), anchor, "anchor_stage_incomplete");
            }
            int anchorIndex = VIDEO_STAGE_GRAPH.indexOf(anchor.stage());
            for (int index = anchorIndex + 1; index < VIDEO_STAGE_GRAPH.size(); index++) {
                String stage = VIDEO_STAGE_GRAPH.get(index);
                StageSnapshot snapshot = snapshots.get(stage);
                if (snapshot == null) {
                    return new ResumeProbe(stage, ownerOf(stage), null, "next_stage_missing");
                }
                if (!isCompletedStatus(snapshot.status())) {
                    return new ResumeProbe(stage, firstNonBlank(snapshot.stageOwner(), ownerOf(stage)), snapshot, "next_stage_incomplete");
                }
            }
            return new ResumeProbe(anchor.stage(), firstNonBlank(anchor.stageOwner(), ownerOf(anchor.stage())), anchor, "pipeline_already_completed");
        }
        for (String stage : VIDEO_STAGE_GRAPH) {
            StageSnapshot snapshot = snapshots.get(stage);
            if (snapshot == null) {
                return new ResumeProbe(stage, ownerOf(stage), null, "full_scan_missing_stage");
            }
            if (!isCompletedStatus(snapshot.status())) {
                return new ResumeProbe(stage, firstNonBlank(snapshot.stageOwner(), ownerOf(stage)), snapshot, "full_scan_incomplete_stage");
            }
        }
        return new ResumeProbe("download", ownerOf("download"), null, "no_runtime_state");
    }

    private StageSnapshot parseStageSnapshot(Path stageStatePath) {
        try {
            JsonNode root = objectMapper.readTree(Files.readAllBytes(stageStatePath));
            if (root == null || !root.isObject()) {
                return null;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = objectMapper.convertValue(root, Map.class);
            long updatedAtMs = readLong(root, "updated_at_ms");
            if (updatedAtMs <= 0L) {
                updatedAtMs = Files.getLastModifiedTime(stageStatePath).toMillis();
            }
            String stage = normalizeStage(firstNonBlank(readText(root, "stage"), stageStatePath.getParent().getFileName().toString(), ""));
            String stageOwner = firstNonBlank(readText(root, "stage_owner"), readText(root, "owner"), ownerOf(stage));
            String status = firstNonBlank(readText(root, "status"), "UNKNOWN");
            String checkpoint = firstNonBlank(readText(root, "checkpoint"), "unknown");
            String outputDir = firstNonBlank(readText(root, "output_dir"), readText(root, "outputDir"), "");
            payload.putIfAbsent("stage_owner", stageOwner);
            payload.putIfAbsent("stage_state_path", stageStatePath.toAbsolutePath().normalize().toString());
            payload.putIfAbsent("local_stage_state_path", stageStatePath.toAbsolutePath().normalize().toString());
            payload.putIfAbsent("output_dir", outputDir);
            return new StageSnapshot(
                    stage,
                    stageOwner,
                    status,
                    checkpoint,
                    updatedAtMs,
                    outputDir,
                    stageStatePath.toAbsolutePath().normalize().toString(),
                    Collections.unmodifiableMap(payload)
            );
        } catch (Exception error) {
            logger.debug("Parse runtime stage state skipped: path={} err={}", stageStatePath, error.getMessage());
            return null;
        }
    }

    private StageSnapshot findSnapshotByStageStatePath(
            Path taskDir,
            Map<String, StageSnapshot> snapshots,
            String rawStageStatePath
    ) {
        String normalizedPath = normalizeComparablePath(taskDir, rawStageStatePath);
        if (normalizedPath.isBlank() || snapshots == null || snapshots.isEmpty()) {
            return null;
        }
        for (StageSnapshot snapshot : snapshots.values()) {
            if (snapshot == null) {
                continue;
            }
            String snapshotPath = firstNonBlank(snapshot.stageStatePath(), snapshot.readText("stage_state_path", "local_stage_state_path"));
            if (!snapshotPath.isBlank() && normalizeComparablePath(taskDir, snapshotPath).equals(normalizedPath)) {
                return snapshot;
            }
        }
        return null;
    }

    private String normalizeComparablePath(Path taskDir, String rawPath) {
        String normalized = trimToNull(rawPath);
        if (normalized == null) {
            return "";
        }
        try {
            Path path = Paths.get(normalized);
            if (!path.isAbsolute() && taskDir != null) {
                path = taskDir.resolve(path);
            }
            String normalizedPath = path.toAbsolutePath().normalize().toString();
            return File.separatorChar == '\\' ? normalizedPath.toLowerCase(Locale.ROOT) : normalizedPath;
        } catch (Exception ignored) {
            return "";
        }
    }

    private List<Path> resolveCandidateTaskDirs(String videoUrl, String outputDir, String resultPath) {
        LinkedHashSet<Path> candidates = new LinkedHashSet<>();
        addCandidateDir(candidates, outputDir);
        addCandidateDir(candidates, resolveTaskRootFromResultPath(resultPath));
        Path storageRoot = resolveStorageRoot();
        if (storageRoot != null) {
            String normalizedInput = firstNonBlank(videoUrl, "");
            if (!normalizedInput.isBlank()) {
                String hashSource = isHttpUrl(normalizedInput)
                        ? buildDownloadTaskDirSource(normalizedInput)
                        : normalizePathForHash(normalizedInput);
                candidates.add(storageRoot.resolve(md5Hex(hashSource)));
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
            logger.debug("Skip invalid recovery candidate path: rawPath={}", rawPath);
        }
    }

    private FallbackRepairDirective loadFallbackRepairDirective(Path taskDir) {
        if (taskDir == null || !Files.isDirectory(taskDir)) {
            return null;
        }
        Path auditPath = taskDir.resolve("intermediates").resolve("rt").resolve(FALLBACK_AUDIT_FILE_NAME);
        if (!Files.isRegularFile(auditPath)) {
            Path legacyAuditPath = taskDir.resolve("intermediates").resolve(LEGACY_FALLBACK_AUDIT_FILE_NAME);
            if (!Files.isRegularFile(legacyAuditPath)) {
                return null;
            }
            auditPath = legacyAuditPath;
        }
        LinkedHashSet<String> scopeRefs = new LinkedHashSet<>();
        long updatedAtMs = 0L;
        String repairStage = "";
        String repairReason = "";
        int eventCount = 0;
        try (BufferedReader reader = Files.newBufferedReader(auditPath, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String normalizedLine = line != null ? line.trim() : "";
                if (normalizedLine.isEmpty()) {
                    continue;
                }
                JsonNode root = objectMapper.readTree(normalizedLine);
                if (root == null || !root.isObject()) {
                    continue;
                }
                JsonNode fallbackNode = root.path("fallback");
                if (!fallbackNode.path("is_fallback").asBoolean(false)) {
                    continue;
                }
                List<String> eventScopeRefs = readStringList(fallbackNode, "propagated_scope_refs");
                if (eventScopeRefs.isEmpty()) {
                    continue;
                }
                eventCount += 1;
                scopeRefs.addAll(eventScopeRefs);
                long eventUpdatedAt = readTimestampMs(root);
                if (eventUpdatedAt >= updatedAtMs) {
                    updatedAtMs = eventUpdatedAt;
                    repairStage = firstNonBlank(
                            readText(fallbackNode, "repair_stage"),
                            readText(root, "stage"),
                            repairStage
                    );
                    repairReason = firstNonBlank(
                            readText(fallbackNode, "fallback_reason"),
                            readText(fallbackNode, "summary"),
                            repairReason
                    );
                }
            }
        } catch (Exception error) {
            logger.debug("Load fallback repair directive skipped: path={} err={}", auditPath, error.getMessage());
            return null;
        }
        if (scopeRefs.isEmpty()) {
            return null;
        }
        return new FallbackRepairDirective(
                taskDir,
                firstNonBlank(repairStage, "phase2b"),
                "fallback_propagation",
                List.copyOf(scopeRefs),
                firstNonBlank(repairReason, "repair recorded fallback propagation path"),
                eventCount,
                updatedAtMs
        );
    }

    private long readTimestampMs(JsonNode root) {
        if (root == null || root.isMissingNode()) {
            return 0L;
        }
        long updatedAtMs = root.path("updated_at_ms").asLong(0L);
        if (updatedAtMs > 0L) {
            return updatedAtMs;
        }
        String rawTimestamp = firstNonBlank(readText(root, "timestamp"), "");
        if (rawTimestamp.isBlank()) {
            return 0L;
        }
        try {
            return java.time.OffsetDateTime.parse(rawTimestamp).toInstant().toEpochMilli();
        } catch (Exception ignored) {
            return 0L;
        }
    }

    private List<String> readStringList(JsonNode root, String fieldName) {
        if (root == null || root.isMissingNode() || fieldName == null || fieldName.isBlank()) {
            return List.of();
        }
        JsonNode node = root.path(fieldName);
        if (!node.isArray()) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        for (JsonNode item : node) {
            String value = trimToNull(item != null ? item.asText("") : "");
            if (value != null) {
                values.add(value);
            }
        }
        return values;
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
            return path.getParent() != null ? path.getParent().toString() : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    private Path resolveStorageRoot() {
        String configured = trimToNull(configuredStorageRoot);
        if (configured != null) {
            try { return Paths.get(configured).toAbsolutePath().normalize(); } catch (Exception ignored) { }
        }
        String envRoot = trimToNull(System.getenv("V2M_STORAGE_ROOT"));
        if (envRoot != null) {
            try { return Paths.get(envRoot).toAbsolutePath().normalize(); } catch (Exception ignored) { }
        }
        return Paths.get("var", "storage", "storage").toAbsolutePath().normalize();
    }

    private boolean isHttpUrl(String value) {
        if (value == null || value.isBlank()) { return false; }
        try {
            String scheme = URI.create(value).getScheme();
            return scheme != null && ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme));
        } catch (Exception ignored) { return false; }
    }

    private String buildDownloadTaskDirSource(String videoUrl) {
        String bilibiliVideoId = extractBilibiliVideoId(videoUrl);
        if (bilibiliVideoId == null || bilibiliVideoId.isBlank()) {
            return videoUrl == null ? "" : videoUrl;
        }
        Integer episodeIndex = extractBilibiliEpisodeIndex(videoUrl);
        return episodeIndex != null && episodeIndex > 0 ? bilibiliVideoId + "_" + episodeIndex : bilibiliVideoId;
    }

    private String extractBilibiliVideoId(String videoUrl) {
        if (videoUrl == null || videoUrl.isBlank()) { return null; }
        try {
            URI parsed = URI.create(videoUrl);
            if (!isBilibiliHost(parsed.getHost())) { return null; }
            Map<String, String> query = parseQueryParams(parsed.getRawQuery());
            String bvid = query.getOrDefault("bvid", "");
            if (!bvid.isBlank()) {
                Matcher matcher = BILIBILI_BV_PATTERN.matcher(bvid);
                if (matcher.find()) { return matcher.group(); }
            }
            String aid = query.getOrDefault("aid", "");
            if (!aid.isBlank() && aid.chars().allMatch(Character::isDigit)) { return "AV" + aid; }
            String searchSpace = String.join(" ", firstNonBlank(parsed.getRawPath(), ""), firstNonBlank(parsed.getRawQuery(), ""), firstNonBlank(parsed.getRawFragment(), ""));
            Matcher bvMatcher = BILIBILI_BV_PATTERN.matcher(searchSpace);
            if (bvMatcher.find()) { return bvMatcher.group(); }
            Matcher avMatcher = BILIBILI_AV_PATTERN.matcher(searchSpace);
            return avMatcher.find() ? "AV" + avMatcher.group(1) : null;
        } catch (Exception ignored) { return null; }
    }

    private Integer extractBilibiliEpisodeIndex(String videoUrl) {
        if (videoUrl == null || videoUrl.isBlank()) { return null; }
        try {
            URI parsed = URI.create(videoUrl);
            if (!isBilibiliHost(parsed.getHost())) { return null; }
            String rawEpisode = firstNonBlank(parseQueryParams(parsed.getRawQuery()).get("p"), "");
            if (rawEpisode.isBlank()) { return null; }
            int episodeIndex = Integer.parseInt(rawEpisode);
            return episodeIndex > 0 ? episodeIndex : null;
        } catch (Exception ignored) { return null; }
    }

    private boolean isBilibiliHost(String host) {
        if (host == null || host.isBlank()) { return false; }
        String normalized = host.toLowerCase(Locale.ROOT).split(":", 2)[0];
        return normalized.equals("bilibili.com") || normalized.endsWith(".bilibili.com")
                || normalized.equals("b23.tv") || normalized.endsWith(".b23.tv");
    }

    private Map<String, String> parseQueryParams(String rawQuery) {
        Map<String, String> params = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isBlank()) { return params; }
        for (String pair : rawQuery.split("&")) {
            if (pair == null || pair.isBlank()) { continue; }
            String[] tokens = pair.split("=", 2);
            String key = decodeUrl(tokens[0]);
            String value = tokens.length > 1 ? decodeUrl(tokens[1]) : "";
            if (!key.isBlank() && !params.containsKey(key)) { params.put(key, value); }
        }
        return params;
    }

    private String decodeUrl(String value) {
        try { return value == null ? "" : java.net.URLDecoder.decode(value, StandardCharsets.UTF_8); }
        catch (Exception ignored) { return value == null ? "" : value; }
    }

    private String normalizePathForHash(String path) {
        String normalized = new File(path).getAbsolutePath().replace('/', File.separatorChar);
        return File.separatorChar == '\\' ? normalized.toLowerCase(Locale.ROOT) : normalized;
    }

    private String md5Hex(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("MD5").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) { sb.append(String.format("%02x", b)); }
            return sb.toString();
        } catch (Exception error) { throw new IllegalStateException("md5 hash failed", error); }
    }

    private String readText(JsonNode node, String fieldName) {
        if (node == null || fieldName == null || fieldName.isBlank()) { return ""; }
        JsonNode child = node.get(fieldName);
        if (child == null || child.isNull()) { return ""; }
        return child.isTextual() ? child.asText("") : child.toString();
    }

    private long readLong(JsonNode node, String fieldName) {
        if (node == null || fieldName == null || fieldName.isBlank()) { return 0L; }
        JsonNode child = node.get(fieldName);
        if (child == null || child.isNull()) { return 0L; }
        if (child.isNumber()) { return child.asLong(0L); }
        try { return Long.parseLong(child.asText("0").trim()); } catch (Exception ignored) { return 0L; }
    }

    private boolean isCompletedStatus(String status) { return "SUCCESS".equals(normalizeStatus(status)); }
    private String ownerOf(String stage) { return "asset_extract_java".equals(normalizeStage(stage)) ? "java" : "python"; }
    private String normalizeStage(String stage) { String value = trimToNull(stage); return value == null ? "" : value.toLowerCase(Locale.ROOT); }
    private String normalizeStatus(String status) {
        String value = trimToNull(status);
        if (value == null) {
            return "";
        }
        String normalized = value.toUpperCase(Locale.ROOT);
        if (Set.of("PLANNED", "PLANNING", "RETRYING", "RETRING").contains(normalized)) {
            return "PLANNED";
        }
        if (Set.of("RUNNING", "RUNING", "EXECUTING", "LOCAL_WRITING").contains(normalized)) {
            return "RUNNING";
        }
        if (Set.of("SUCCESS", "COMPLETED", "LOCAL_COMMITTED").contains(normalized)) {
            return "SUCCESS";
        }
        if (Set.of("MANUAL_NEEDED", "MANUAL_RETRY_REQUIRED", "MANUL_NEEDED").contains(normalized)) {
            return "MANUAL_NEEDED";
        }
        if (Set.of("ERROR", "AUTO_RETRY_WAIT").contains(normalized)) {
            return "ERROR";
        }
        if (Set.of("FAILED", "FATAL", "FAIL").contains(normalized)) {
            return "FAILED";
        }
        return normalized;
    }
    private String firstNonBlank(String... values) { for (String value : values) { String v = trimToNull(value); if (v != null) { return v; } } return ""; }
    private String trimToNull(String value) { if (value == null) { return null; } String trimmed = value.trim(); return trimmed.isEmpty() ? null : trimmed; }
    private StageSnapshot latestSnapshot(Collection<StageSnapshot> snapshots) { return snapshots.stream().max(Comparator.comparingLong(StageSnapshot::updatedAtMs)).orElse(null); }

    private record HintCandidate(String stage, String stageStatePath) { }
    private record ResumeProbe(String resumeFromStage, String stageOwner, StageSnapshot resumeSnapshot, String reason) { }

    public record StageSnapshot(
            String stage,
            String stageOwner,
            String status,
            String checkpoint,
            long updatedAtMs,
            String outputDir,
            String stageStatePath,
            Map<String, Object> payload
    ) {
        public RecoveryDirective toRecoveryDirective() {
            return new RecoveryDirective(
                    stage, status, checkpoint, updatedAtMs,
                    readText("retry_mode", "retryMode"),
                    readText("required_action", "requiredAction"),
                    readText("retry_entry_point", "retryEntryPoint"),
                    readText("retry_strategy", "retryStrategy"),
                    readText("operator_action", "operatorAction"),
                    readText("action_hint", "actionHint"),
                    readText("error_class", "errorClass"),
                    readText("error_message", "errorMessage"),
                    outputDir, stageStatePath
            );
        }
        public String readText(String... fields) {
            if (payload == null || fields == null) { return ""; }
            for (String field : fields) {
                Object value = payload.get(field);
                if (value == null) { continue; }
                String normalized = String.valueOf(value).trim();
                if (!normalized.isEmpty()) { return normalized; }
            }
            return "";
        }
        public long readLong(String... fields) {
            if (payload == null || fields == null) { return 0L; }
            for (String field : fields) {
                Object value = payload.get(field);
                if (value instanceof Number numberValue) { return numberValue.longValue(); }
                if (value == null) { continue; }
                try { return Long.parseLong(String.valueOf(value).trim()); } catch (Exception ignored) { }
            }
            return 0L;
        }
        public double readDouble(String... fields) {
            if (payload == null || fields == null) { return 0.0d; }
            for (String field : fields) {
                Object value = payload.get(field);
                if (value instanceof Number numberValue) { return numberValue.doubleValue(); }
                if (value == null) { continue; }
                try { return Double.parseDouble(String.valueOf(value).trim()); } catch (Exception ignored) { }
            }
            return 0.0d;
        }
    }

    public record ResumeDecision(
            Path taskDir,
            String resumeFromStage,
            String stageOwner,
            StageSnapshot resumeStageSnapshot,
            StageSnapshot hintedStageSnapshot,
            StageSnapshot latestStageSnapshot,
            Map<String, StageSnapshot> stageSnapshots,
            String reason
    ) {
        public StageSnapshot stageSnapshot(String stage) { return stage == null || stage.isBlank() ? null : stageSnapshots.get(stage.trim().toLowerCase(Locale.ROOT)); }
        public String findText(String... fields) { for (StageSnapshot s : orderedSnapshots()) { String v = s.readText(fields); if (!v.isBlank()) { return v; } } return ""; }
        public long findLong(String... fields) { for (StageSnapshot s : orderedSnapshots()) { long v = s.readLong(fields); if (v > 0L) { return v; } } return 0L; }
        public double findDouble(String... fields) { for (StageSnapshot s : orderedSnapshots()) { double v = s.readDouble(fields); if (v > 0.0d) { return v; } } return 0.0d; }
        private List<StageSnapshot> orderedSnapshots() {
            if (stageSnapshots == null || stageSnapshots.isEmpty()) { return Collections.emptyList(); }
            List<StageSnapshot> ordered = new ArrayList<>();
            for (int i = VIDEO_STAGE_GRAPH.size() - 1; i >= 0; i--) {
                StageSnapshot snapshot = stageSnapshots.get(VIDEO_STAGE_GRAPH.get(i));
                if (snapshot != null) { ordered.add(snapshot); }
            }
            stageSnapshots.values().stream()
                    .filter(snapshot -> !ordered.contains(snapshot))
                    .sorted(Comparator.comparingLong(StageSnapshot::updatedAtMs).reversed())
                    .forEach(ordered::add);
            return ordered;
        }
    }

    public record FallbackRepairDirective(
            Path taskDir,
            String stage,
            String repairMode,
            List<String> scopeRefs,
            String reason,
            int eventCount,
            long updatedAtMs
    ) {
        public Map<String, Object> toPayload() {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("repairMode", RecoveryDirective.firstNonBlank(repairMode, ""));
            payload.put("repairStage", RecoveryDirective.firstNonBlank(stage, ""));
            payload.put("repairReason", RecoveryDirective.firstNonBlank(reason, ""));
            payload.put("repairScopeCount", Math.max(0, scopeRefs != null ? scopeRefs.size() : 0));
            payload.put("repairScopeRefs", scopeRefs != null ? List.copyOf(scopeRefs) : List.of());
            payload.put("fallbackEventCount", Math.max(0, eventCount));
            payload.put("updatedAtMs", Math.max(0L, updatedAtMs));
            return payload;
        }
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
            return String.format(
                    "[%s/%s] %s",
                    firstNonBlank(stage, "unknown_stage"),
                    firstNonBlank(checkpoint, "unknown"),
                    firstNonBlank(requiredAction, actionHint, errorMessage, "需要人工检查后重试")
            );
        }
        public static String firstNonBlank(String... values) {
            if (values == null) { return ""; }
            for (String value : values) {
                if (value == null) { continue; }
                String trimmed = value.trim();
                if (!trimmed.isEmpty()) { return trimmed; }
            }
            return "";
        }
    }
}
