package com.mvp.module2.fusion.service;

import com.mvp.module2.fusion.queue.TaskQueueManager;
import com.mvp.module2.fusion.service.TaskCleanupQueueRepository.PendingCleanupTaskRecord;
import com.mvp.module2.fusion.service.TaskStateRepository.PersistedTaskRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

@Service
public class TaskCleanupIndexService {

    private static final Logger logger = LoggerFactory.getLogger(TaskCleanupIndexService.class);
    private static final String POLICY_VERSION = "task_cleanup_latest_layout_v1";
    private static final String TASK_TYPE_VIDEO = "VIDEO";
    private static final String TASK_TYPE_BOOK = "BOOK";
    private static final Set<String> COMPLETABLE_STATUSES = Set.of("COMPLETED");
    private static final List<String> ROOT_MARKDOWN_CANDIDATES = List.of(
            "enhanced_output.md",
            "book.md",
            "book_enhanced.md"
    );
    private static final List<String> BOOK_INTERMEDIATE_DIRS = List.of(
            "intermediates/book_enhanced",
            "intermediates/book_mineru_page_slices",
            "intermediates/book_mineru_raw",
            "intermediates/book_pdf_extract",
            "intermediates/book_pdf_slices"
    );
    private static final List<String> LEGACY_INTERMEDIATE_FILES = List.of(
            "intermediates/sentence_timestamps.json",
            "intermediates/step1_validate_output.json",
            "intermediates/step2_correction_output.json",
            "intermediates/step2_output.json",
            "intermediates/step3_merge_output.json",
            "intermediates/step3_output.json",
            "intermediates/step3_5_translate_output.json",
            "intermediates/step3_5_output.json",
            "intermediates/step4_clean_local_output.json",
            "intermediates/step4_output.json",
            "intermediates/step5_6_dedup_merge_output.json",
            "intermediates/step6_merge_cross_output.json",
            "intermediates/step6_output.json",
            "intermediates/semantic_units_phase2a.json",
            "intermediates/semantic_units_vl_subset.json",
            "intermediates/vl_analysis_output_latest.json",
            "intermediates/vl_analysis_cache.json",
            "intermediates/phase2b_image_match_audit.json"
    );

    private final TaskStateRepository taskStateRepository;
    private final TaskCleanupQueueRepository taskCleanupQueueRepository;
    private Clock clock = Clock.systemDefaultZone();

    @Autowired(required = false)
    private StorageTaskCacheService storageTaskCacheService;

    @Autowired(required = false)
    private TaskRuntimeStageStore taskRuntimeStageStore;

    @Value("${task.cleanup.enabled:true}")
    private boolean cleanupEnabled;

    @Value("${task.cleanup.completed-ttl-hours:168}")
    private long completedTtlHours;

    @Value("${task.cleanup.window.start-hour:0}")
    private int cleanupWindowStartHour;

    @Value("${task.cleanup.window.end-hour:5}")
    private int cleanupWindowEndHour;

    @Value("${task.cleanup.scan.batch-size:8}")
    private int cleanupBatchSize;

    @Value("${task.cleanup.window.zone-id:}")
    private String configuredCleanupZoneId;

    @Value("${task.storage.root:}")
    private String configuredStorageRoot;

    public TaskCleanupIndexService(
            TaskStateRepository taskStateRepository,
            TaskCleanupQueueRepository taskCleanupQueueRepository
    ) {
        this.taskStateRepository = taskStateRepository;
        this.taskCleanupQueueRepository = taskCleanupQueueRepository;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void reconcilePendingCleanupPolicyOnStartup() {
        if (!cleanupEnabled) {
            return;
        }
        int updated = reconcileCleanupPolicy();
        if (updated > 0) {
            logger.info("Task cleanup policy reconciled on startup: updatedRows={} ttlHours={}", updated, completedTtlHours);
        }
    }

    @Transactional
    public void persistTaskState(TaskQueueManager.TaskEntry task) {
        if (task == null) {
            return;
        }
        taskStateRepository.upsertTask(task);
        syncCleanupIndex(task);
    }

    @Transactional
    public void removeTaskFromCleanupIndex(String taskId) {
        taskCleanupQueueRepository.delete(taskId);
    }

    public int reconcileCleanupPolicy() {
        if (!cleanupEnabled) {
            return 0;
        }
        return taskCleanupQueueRepository.refreshPolicy(
                POLICY_VERSION,
                ttlMillis(),
                Instant.now(clock).toEpochMilli()
        );
    }

    @Scheduled(
            initialDelayString = "${task.cleanup.scan.initial-delay-ms:180000}",
            fixedDelayString = "${task.cleanup.scan.fixed-delay-ms:900000}"
    )
    public void scanAndCleanupDueTasks() {
        runDueCleanupPass(Instant.now(clock), false);
    }

    CleanupRunSummary runDueCleanupPass(Instant now, boolean ignoreWindow) {
        CleanupRunSummary summary = new CleanupRunSummary();
        if (!cleanupEnabled) {
            return summary;
        }
        reconcileCleanupPolicy();
        Instant effectiveNow = now != null ? now : Instant.now(clock);
        if (!ignoreWindow && !isWithinCleanupWindow(effectiveNow)) {
            return summary;
        }
        List<PendingCleanupTaskRecord> dueTasks = taskCleanupQueueRepository.listDue(
                effectiveNow.toEpochMilli(),
                Math.max(1, cleanupBatchSize)
        );
        summary.scanned = dueTasks.size();
        for (PendingCleanupTaskRecord record : dueTasks) {
            CleanupOutcome outcome = cleanupOne(record);
            summary.deletedEntries += outcome.deletedEntries;
            if (outcome.removedFromQueue) {
                summary.removedFromQueue += 1;
            }
            if (outcome.cleanupPerformed) {
                summary.cleanedTasks += 1;
            }
            if (outcome.failed) {
                summary.failedTasks += 1;
            }
        }
        if (summary.scanned > 0) {
            logger.info(
                    "Task cleanup scan finished: scanned={} cleaned={} queueRemoved={} deletedEntries={} failed={}",
                    summary.scanned,
                    summary.cleanedTasks,
                    summary.removedFromQueue,
                    summary.deletedEntries,
                    summary.failedTasks
            );
        }
        return summary;
    }

    private CleanupOutcome cleanupOne(PendingCleanupTaskRecord record) {
        CleanupOutcome outcome = new CleanupOutcome();
        if (record == null || isBlank(record.taskId())) {
            return outcome;
        }
        Optional<PersistedTaskRecord> persistedTask = taskStateRepository.findTask(record.taskId());
        if (persistedTask.isPresent() && !COMPLETABLE_STATUSES.contains(normalizeUpper(persistedTask.get().status))) {
            taskCleanupQueueRepository.delete(record.taskId());
            outcome.removedFromQueue = true;
            return outcome;
        }
        Path taskRoot = normalizePath(record.taskRoot());
        if (taskRoot == null || !Files.isDirectory(taskRoot) || !isManagedStorageTaskRoot(taskRoot)) {
            taskCleanupQueueRepository.delete(record.taskId());
            outcome.removedFromQueue = true;
            return outcome;
        }

        try {
            CleanupPlan plan = buildCleanupPlan(taskRoot, record.taskType());
            if (plan.targets().isEmpty()) {
                taskCleanupQueueRepository.delete(record.taskId());
                outcome.removedFromQueue = true;
                return outcome;
            }
            for (Path target : plan.targets()) {
                outcome.deletedEntries += deletePathRecursively(target, taskRoot);
            }
            taskCleanupQueueRepository.delete(record.taskId());
            outcome.cleanupPerformed = true;
            outcome.removedFromQueue = true;
            return outcome;
        } catch (Exception error) {
            long updatedAtMs = Instant.now(clock).toEpochMilli();
            taskCleanupQueueRepository.markFailure(record.taskId(), error.getMessage(), updatedAtMs);
            outcome.failed = true;
            logger.warn(
                    "Task cleanup failed: taskId={} taskRoot={} err={}",
                    record.taskId(),
                    record.taskRoot(),
                    error.getMessage()
            );
            return outcome;
        }
    }

    private void syncCleanupIndex(TaskQueueManager.TaskEntry task) {
        if (task == null || isBlank(task.taskId)) {
            return;
        }
        PendingCleanupTaskRecord candidate = buildCleanupCandidate(task);
        if (candidate == null) {
            taskCleanupQueueRepository.delete(task.taskId);
            return;
        }
        taskCleanupQueueRepository.upsert(candidate);
    }

    private PendingCleanupTaskRecord buildCleanupCandidate(TaskQueueManager.TaskEntry task) {
        if (!cleanupEnabled || task == null || task.status == null) {
            return null;
        }
        if (!COMPLETABLE_STATUSES.contains(normalizeUpper(task.status.name()))) {
            return null;
        }
        if (task.completedAt == null) {
            return null;
        }
        Path taskRoot = resolveTaskRoot(task);
        if (taskRoot == null || !Files.isDirectory(taskRoot) || !isManagedStorageTaskRoot(taskRoot)) {
            return null;
        }
        if (!matchesLatestManagedLayout(taskRoot)) {
            return null;
        }
        String storageKey = resolveStorageKey(taskRoot);
        if (storageKey == null) {
            return null;
        }
        long completedAtMs = task.completedAt.toEpochMilli();
        long ttlMillis = ttlMillis();
        long nowMs = Instant.now(clock).toEpochMilli();
        return new PendingCleanupTaskRecord(
                task.taskId,
                storageKey,
                taskRoot.toString(),
                resolveTaskType(task, taskRoot),
                task.status.name(),
                POLICY_VERSION,
                ttlMillis,
                completedAtMs,
                completedAtMs + ttlMillis,
                nowMs,
                null
        );
    }

    private CleanupPlan buildCleanupPlan(Path taskRoot, String taskType) throws IOException {
        LinkedHashSet<Path> targets = new LinkedHashSet<>();
        collectCleanupTargets(targets, taskRoot, "intermediates/rt");
        collectCleanupTargets(targets, taskRoot, "local_storage");
        collectCleanupTargets(targets, taskRoot, "intermediates/stages/stage1");
        collectCleanupTargets(targets, taskRoot, "intermediates/stages/phase2a/outputs");
        collectCleanupTargets(targets, taskRoot, "intermediates/stages/phase2b");

        Path phase2aAudits = taskRoot.resolve("intermediates").resolve("stages").resolve("phase2a").resolve("audits").normalize();
        if (Files.isDirectory(phase2aAudits)) {
            try (Stream<Path> pathStream = Files.list(phase2aAudits)) {
                pathStream
                        .filter(path -> !Objects.equals(path.getFileName().toString(), "token_cost_audit.json"))
                        .forEach(targets::add);
            }
        }

        for (String relativePath : LEGACY_INTERMEDIATE_FILES) {
            collectCleanupTargets(targets, taskRoot, relativePath);
        }
        if (TASK_TYPE_BOOK.equalsIgnoreCase(trim(taskType))) {
            for (String relativeDir : BOOK_INTERMEDIATE_DIRS) {
                collectCleanupTargets(targets, taskRoot, relativeDir);
            }
        }

        Path intermediatesRoot = taskRoot.resolve("intermediates").normalize();
        if (Files.isDirectory(intermediatesRoot)) {
            try (Stream<Path> metricsStream = Files.list(intermediatesRoot)) {
                metricsStream
                        .filter(Files::isRegularFile)
                        .filter(path -> {
                            String name = path.getFileName().toString();
                            return name.startsWith("task_metrics_") && !"task_metrics_latest.json".equals(name);
                        })
                        .forEach(targets::add);
            }
        }

        try (Stream<Path> metaStream = Files.walk(taskRoot)) {
            metaStream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".meta.json"))
                    .forEach(targets::add);
        }

        return new CleanupPlan(pruneNestedTargets(targets, taskRoot));
    }

    private List<Path> pruneNestedTargets(Collection<Path> rawTargets, Path taskRoot) {
        List<Path> candidates = new ArrayList<>();
        if (rawTargets == null || taskRoot == null) {
            return candidates;
        }
        Path normalizedRoot = taskRoot.toAbsolutePath().normalize();
        rawTargets.stream()
                .filter(Objects::nonNull)
                .map(path -> path.toAbsolutePath().normalize())
                .filter(path -> path.startsWith(normalizedRoot))
                .sorted(Comparator.comparingInt(Path::getNameCount))
                .forEach(candidate -> {
                    for (Path existing : candidates) {
                        if (candidate.startsWith(existing)) {
                            return;
                        }
                    }
                    candidates.add(candidate);
                });
        candidates.sort(Comparator.comparingInt(Path::getNameCount).reversed());
        return candidates;
    }

    private void collectCleanupTargets(Set<Path> targets, Path taskRoot, String relativePath) {
        if (targets == null || taskRoot == null || isBlank(relativePath)) {
            return;
        }
        Path candidate = taskRoot.resolve(relativePath).normalize();
        if (candidate.startsWith(taskRoot) && Files.exists(candidate)) {
            targets.add(candidate);
        }
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
        int deletedCount = 0;
        try (Stream<Path> pathStream = Files.walk(normalizedTarget)) {
            List<Path> deleteOrder = pathStream.sorted(Comparator.reverseOrder()).toList();
            for (Path path : deleteOrder) {
                if (Files.deleteIfExists(path)) {
                    deletedCount += 1;
                }
            }
        }
        return deletedCount;
    }

    private boolean matchesLatestManagedLayout(Path taskRoot) {
        if (taskRoot == null) {
            return false;
        }
        Path intermediatesRoot = taskRoot.resolve("intermediates").normalize();
        if (!Files.isDirectory(intermediatesRoot)) {
            return false;
        }
        if (Files.isRegularFile(intermediatesRoot.resolve("task_metrics_latest.json"))) {
            return true;
        }
        for (String candidateName : ROOT_MARKDOWN_CANDIDATES) {
            if (Files.isRegularFile(taskRoot.resolve(candidateName).normalize())) {
                return true;
            }
        }
        if (taskRuntimeStageStore != null) {
            try {
                if (taskRuntimeStageStore.hasProjectionPayload(taskRoot.toString(), "phase2b", "result_document")) {
                    return true;
                }
            } catch (Exception error) {
                logger.debug("Ignore runtime artifact layout probe failure: taskRoot={}", taskRoot, error);
            }
        }
        return Files.isRegularFile(taskRoot.resolve("result.json").normalize())
                || Files.isRegularFile(taskRoot.resolve("video_meta.json").normalize());
    }

    private String resolveTaskType(TaskQueueManager.TaskEntry task, Path taskRoot) {
        if (task != null && task.bookOptions != null) {
            return TASK_TYPE_BOOK;
        }
        if (taskRoot != null) {
            if (Files.isRegularFile(taskRoot.resolve("book.md").normalize())
                    || Files.isRegularFile(taskRoot.resolve("book_enhanced.md").normalize())
                    || Files.isRegularFile(taskRoot.resolve("book_semantic_units.json").normalize())
                    || Files.isRegularFile(taskRoot.resolve("book_enhanced_semantic_units.json").normalize())) {
                return TASK_TYPE_BOOK;
            }
        }
        return TASK_TYPE_VIDEO;
    }

    private Path resolveTaskRoot(TaskQueueManager.TaskEntry task) {
        Path outputDirPath = normalizePath(task.outputDir);
        if (outputDirPath != null && Files.isDirectory(outputDirPath)) {
            return outputDirPath;
        }
        Path resultPath = normalizePath(task.resultPath);
        if (resultPath != null) {
            if (Files.isDirectory(resultPath)) {
                return resultPath;
            }
            Path parent = resultPath.getParent();
            if (parent != null && Files.isDirectory(parent)) {
                return parent;
            }
        }
        return outputDirPath;
    }

    private boolean isManagedStorageTaskRoot(Path taskRoot) {
        Path storageRoot = resolveStorageRoot();
        if (taskRoot == null || storageRoot == null) {
            return false;
        }
        Path normalizedRoot = storageRoot.toAbsolutePath().normalize();
        Path normalizedTaskRoot = taskRoot.toAbsolutePath().normalize();
        return normalizedTaskRoot.startsWith(normalizedRoot)
                && !normalizedRoot.equals(normalizedTaskRoot)
                && resolveStorageKey(normalizedTaskRoot) != null;
    }

    private String resolveStorageKey(Path taskRoot) {
        Path storageRoot = resolveStorageRoot();
        if (taskRoot == null || storageRoot == null) {
            return null;
        }
        Path normalizedRoot = storageRoot.toAbsolutePath().normalize();
        Path normalizedTaskRoot = taskRoot.toAbsolutePath().normalize();
        if (!normalizedTaskRoot.startsWith(normalizedRoot)) {
            return null;
        }
        Path relative = normalizedRoot.relativize(normalizedTaskRoot);
        if (relative.getNameCount() == 0) {
            return null;
        }
        String storageKey = trim(relative.getName(0).toString());
        return isSafeStorageKey(storageKey) ? storageKey : null;
    }

    private boolean isWithinCleanupWindow(Instant now) {
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(now, resolveCleanupZoneId());
        int hour = dateTime.getHour();
        int startHour = clampHour(cleanupWindowStartHour);
        int endHour = clampHour(cleanupWindowEndHour);
        if (startHour == endHour) {
            return true;
        }
        if (startHour < endHour) {
            return hour >= startHour && hour < endHour;
        }
        return hour >= startHour || hour < endHour;
    }

    private ZoneId resolveCleanupZoneId() {
        String zoneId = trim(configuredCleanupZoneId);
        if (zoneId == null) {
            return clock.getZone();
        }
        try {
            return ZoneId.of(zoneId);
        } catch (Exception ignored) {
            return clock.getZone();
        }
    }

    private Path resolveStorageRoot() {
        if (!isBlank(configuredStorageRoot)) {
            return Paths.get(configuredStorageRoot.trim()).toAbsolutePath().normalize();
        }
        if (storageTaskCacheService != null && storageTaskCacheService.getStorageRoot() != null) {
            return storageTaskCacheService.getStorageRoot().toAbsolutePath().normalize();
        }
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

    private int clampHour(int hour) {
        return Math.min(23, Math.max(0, hour));
    }

    private long ttlMillis() {
        long safeHours = Math.max(1L, completedTtlHours);
        long millisPerHour = 3_600_000L;
        if (safeHours >= Long.MAX_VALUE / millisPerHour) {
            return Long.MAX_VALUE;
        }
        return safeHours * millisPerHour;
    }

    private Path normalizePath(String rawPath) {
        String normalized = trim(rawPath);
        if (normalized == null) {
            return null;
        }
        try {
            return Paths.get(normalized).toAbsolutePath().normalize();
        } catch (Exception ignored) {
            return null;
        }
    }

    private String normalizeUpper(String value) {
        String normalized = trim(value);
        return normalized == null ? "" : normalized.toUpperCase(Locale.ROOT);
    }

    private boolean isSafeStorageKey(String storageKey) {
        return storageKey != null
                && !storageKey.isBlank()
                && !storageKey.contains("..")
                && !storageKey.contains("/")
                && !storageKey.contains("\\");
    }

    private String trim(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private boolean isBlank(String value) {
        return trim(value) == null;
    }

    static final class CleanupRunSummary {
        int scanned;
        int cleanedTasks;
        int removedFromQueue;
        int failedTasks;
        int deletedEntries;
    }

    private static final class CleanupOutcome {
        boolean cleanupPerformed;
        boolean removedFromQueue;
        boolean failed;
        int deletedEntries;
    }

    private record CleanupPlan(List<Path> targets) {
    }
}
