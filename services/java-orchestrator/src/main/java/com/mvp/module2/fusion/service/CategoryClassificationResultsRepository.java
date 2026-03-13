package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * 统一读写 var/storage/category_classification_results.json。
 * 设计目标：
 * 1. 复用 Phase2B 自动分类写出的 results 作为默认合集来源；
 * 2. 允许前端把人工调整后的最终合集路径回写到 collectionBindings；
 * 3. 在不破坏 results 统计字段的前提下，把“最终合集归属”沉淀为单一事实源。
 */
@Service
public class CategoryClassificationResultsRepository {

    private static final Logger logger = LoggerFactory.getLogger(CategoryClassificationResultsRepository.class);
    private static final String SUMMARY_FILE_NAME = "category_classification_results.json";

    private final StorageTaskCacheService storageTaskCacheService;
    private final ObjectMapper objectMapper;

    @Autowired
    public CategoryClassificationResultsRepository(
            StorageTaskCacheService storageTaskCacheService,
            ObjectMapper objectMapper
    ) {
        this.storageTaskCacheService = storageTaskCacheService;
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
    }

    public Map<String, String> findCollectionPathByTaskPaths(Collection<String> rawTaskPaths) {
        if (rawTaskPaths == null || rawTaskPaths.isEmpty()) {
            return Map.of();
        }
        Snapshot snapshot = loadSnapshot();
        Map<String, String> result = new LinkedHashMap<>();
        for (String rawTaskPath : rawTaskPaths) {
            String taskPath = TaskManualCollectionRepository.normalizeTaskPath(rawTaskPath);
            if (taskPath.isEmpty()) {
                continue;
            }
            String collectionPath = snapshot.archivedBindings.get(taskPath);
            if (collectionPath == null || collectionPath.isBlank()) {
                continue;
            }
            result.put(taskPath, collectionPath);
        }
        return result;
    }

    public Map<String, CategoryAssignment> findCategoryAssignmentsByTaskPaths(Collection<String> rawTaskPaths) {
        if (rawTaskPaths == null || rawTaskPaths.isEmpty()) {
            return Map.of();
        }
        Snapshot snapshot = loadSnapshot();
        Map<String, CategoryAssignment> result = new LinkedHashMap<>();
        for (String rawTaskPath : rawTaskPaths) {
            String taskPath = TaskManualCollectionRepository.normalizeTaskPath(rawTaskPath);
            if (taskPath.isEmpty()) {
                continue;
            }
            boolean archived = snapshot.archivedTaskPaths.containsKey(taskPath);
            String categoryPath = archived
                    ? firstNonBlank(snapshot.archivedBindings.get(taskPath), snapshot.automaticBindings.get(taskPath))
                    : snapshot.automaticBindings.get(taskPath);
            String archivedAt = archived ? snapshot.archivedTaskPaths.getOrDefault(taskPath, "") : "";
            boolean manualBinding = archived && snapshot.explicitBindings.containsKey(taskPath);
            result.put(taskPath, new CategoryAssignment(
                    TaskManualCollectionRepository.normalizeCollectionPath(categoryPath),
                    archived,
                    archivedAt,
                    manualBinding
            ));
        }
        return result;
    }

    public Map<String, String> listAllBindings() {
        return loadSnapshot().archivedBindings;
    }

    public Map<String, String> listAutomaticBindings() {
        return loadSnapshot().automaticBindings;
    }

    public Map<String, String> listArchivedTaskPaths() {
        return loadSnapshot().archivedTaskPaths;
    }

    public int replaceAllBindings(Map<String, String> rawBindings) {
        Map<String, String> normalizedArchivedBindings = normalizeBindings(rawBindings);
        ObjectNode root = readOrCreateRoot();
        Map<String, String> automaticBindings = readAutomaticBindings(root.path("results"));
        Map<String, String> existingArchivedTaskPaths = readArchivedTaskPaths(root.path("archivedTaskPaths"));
        String updatedAt = Instant.now().toString();

        ObjectNode bindingsNode = objectMapper.createObjectNode();
        ObjectNode archivedTaskPathsNode = objectMapper.createObjectNode();
        normalizedArchivedBindings.forEach((taskPath, collectionPath) -> {
            String automaticPath = TaskManualCollectionRepository.normalizeCollectionPath(automaticBindings.get(taskPath));
            if (automaticPath.isEmpty() || !automaticPath.equals(collectionPath)) {
                bindingsNode.put(taskPath, collectionPath);
            }
            String archivedAt = firstNonBlank(existingArchivedTaskPaths.get(taskPath), updatedAt);
            archivedTaskPathsNode.put(taskPath, archivedAt);
        });
        root.set("collectionBindings", bindingsNode);
        root.set("archivedTaskPaths", archivedTaskPathsNode);
        root.put("updated_at", updatedAt);
        writeRoot(root);
        return normalizedArchivedBindings.size();
    }

    public long getLastUpdatedEpochMillis() {
        Path summaryPath = resolveSummaryPath();
        long fileModifiedAt = 0L;
        if (summaryPath != null) {
            try {
                if (Files.isRegularFile(summaryPath)) {
                    fileModifiedAt = Files.getLastModifiedTime(summaryPath).toMillis();
                }
            } catch (Exception ex) {
                logger.debug("read category classification summary lastModified failed: {} err={}",
                        summaryPath,
                        ex.getMessage());
            }
        }

        long payloadUpdatedAt = 0L;
        ObjectNode root = readOrCreateRoot();
        if (root.hasNonNull("updated_at")) {
            try {
                payloadUpdatedAt = Instant.parse(root.path("updated_at").asText("")).toEpochMilli();
            } catch (Exception ignored) {
                payloadUpdatedAt = 0L;
            }
        }
        return Math.max(fileModifiedAt, payloadUpdatedAt);
    }

    private Snapshot loadSnapshot() {
        ObjectNode root = readOrCreateRoot();
        Map<String, String> explicitBindings = readCollectionBindings(root.path("collectionBindings"));
        Map<String, String> automaticBindings = readAutomaticBindings(root.path("results"));
        Map<String, String> archivedTaskPaths = readArchivedTaskPaths(root.path("archivedTaskPaths"));
        Map<String, String> archivedBindings = new LinkedHashMap<>();
        archivedTaskPaths.forEach((taskPath, archivedAt) -> {
            String resolvedPath = TaskManualCollectionRepository.normalizeCollectionPath(
                    firstNonBlank(explicitBindings.get(taskPath), automaticBindings.get(taskPath))
            );
            if (resolvedPath.isEmpty()) {
                return;
            }
            archivedBindings.put(taskPath, resolvedPath);
        });
        return new Snapshot(
                Map.copyOf(explicitBindings),
                Map.copyOf(automaticBindings),
                Map.copyOf(archivedTaskPaths),
                Map.copyOf(archivedBindings)
        );
    }

    private Map<String, String> readCollectionBindings(JsonNode bindingsNode) {
        Map<String, String> bindings = new LinkedHashMap<>();
        if (!(bindingsNode instanceof ObjectNode objectNode)) {
            return bindings;
        }
        objectNode.fields().forEachRemaining(entry -> {
            String taskPath = TaskManualCollectionRepository.normalizeTaskPath(entry.getKey());
            String collectionPath = TaskManualCollectionRepository.normalizeCollectionPath(entry.getValue().asText(""));
            if (taskPath.isEmpty() || collectionPath.isEmpty()) {
                return;
            }
            bindings.put(taskPath, collectionPath);
        });
        return bindings;
    }

    private Map<String, String> readAutomaticBindings(JsonNode resultsNode) {
        Map<String, String> bindings = new LinkedHashMap<>();
        if (!resultsNode.isArray()) {
            return bindings;
        }
        for (JsonNode item : resultsNode) {
            if (item == null || !item.isObject()) {
                continue;
            }
            String collectionPath = TaskManualCollectionRepository.normalizeCollectionPath(item.path("category_path").asText(""));
            if (collectionPath.isEmpty()) {
                continue;
            }
            String taskPath = TaskManualCollectionRepository.normalizeTaskPath(
                    firstNonBlank(
                            item.path("task_path").asText(""),
                            item.path("taskPath").asText(""),
                            buildStorageTaskPath(item.path("video_id").asText(""))
                    )
            );
            if (taskPath.isEmpty()) {
                continue;
            }
            bindings.put(taskPath, collectionPath);
        }
        return bindings;
    }

    private Map<String, String> readArchivedTaskPaths(JsonNode archivedNode) {
        Map<String, String> archivedTaskPaths = new LinkedHashMap<>();
        if (!(archivedNode instanceof ObjectNode objectNode)) {
            return archivedTaskPaths;
        }
        objectNode.fields().forEachRemaining(entry -> {
            String taskPath = TaskManualCollectionRepository.normalizeTaskPath(entry.getKey());
            if (taskPath.isEmpty()) {
                return;
            }
            JsonNode value = entry.getValue();
            String archivedAt = "";
            if (value != null && value.isTextual()) {
                archivedAt = value.asText("");
            } else if (value != null && value.asBoolean(false)) {
                archivedAt = Instant.EPOCH.toString();
            }
            archivedTaskPaths.put(taskPath, firstNonBlank(archivedAt, Instant.EPOCH.toString()));
        });
        return archivedTaskPaths;
    }

    private Map<String, String> normalizeBindings(Map<String, String> rawBindings) {
        if (rawBindings == null || rawBindings.isEmpty()) {
            return Map.of();
        }
        Map<String, String> normalized = new LinkedHashMap<>();
        List<String> orderedTaskPaths = new ArrayList<>(new LinkedHashSet<>(rawBindings.keySet()));
        orderedTaskPaths.sort(String::compareTo);
        for (String rawTaskPath : orderedTaskPaths) {
            String taskPath = TaskManualCollectionRepository.normalizeTaskPath(rawTaskPath);
            String collectionPath = TaskManualCollectionRepository.normalizeCollectionPath(rawBindings.get(rawTaskPath));
            if (taskPath.isEmpty() || collectionPath.isEmpty()) {
                continue;
            }
            normalized.put(taskPath, collectionPath);
        }
        return normalized;
    }

    private ObjectNode readOrCreateRoot() {
        Path summaryPath = resolveSummaryPath();
        if (summaryPath == null || !Files.isRegularFile(summaryPath)) {
            return objectMapper.createObjectNode();
        }
        try {
            if (Files.size(summaryPath) == 0L) {
                return objectMapper.createObjectNode();
            }
            JsonNode loaded = objectMapper.readTree(summaryPath.toFile());
            if (loaded instanceof ObjectNode objectNode) {
                return objectNode;
            }
        } catch (Exception ex) {
            logger.warn("read category classification summary failed: {} err={}", summaryPath, ex.getMessage());
        }
        return objectMapper.createObjectNode();
    }

    private void writeRoot(ObjectNode root) {
        Path summaryPath = resolveSummaryPath();
        if (summaryPath == null) {
            logger.warn("skip writing category classification summary: path unavailable");
            return;
        }
        Path parent = summaryPath.getParent();
        if (parent == null) {
            logger.warn("skip writing category classification summary: parent unavailable path={}", summaryPath);
            return;
        }
        Path tmpPath = parent.resolve(summaryPath.getFileName().toString() + ".tmp").normalize();
        try {
            Files.createDirectories(parent);
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmpPath.toFile(), root);
            try {
                Files.move(tmpPath, summaryPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(tmpPath, summaryPath, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (Exception ex) {
            try {
                Files.deleteIfExists(tmpPath);
            } catch (Exception ignored) {
            }
            logger.warn("write category classification summary failed: {} err={}", summaryPath, ex.getMessage());
        }
    }

    private Path resolveSummaryPath() {
        try {
            Path storageRoot = storageTaskCacheService.getStorageRoot();
            if (storageRoot == null) {
                return null;
            }
            Path parent = storageRoot.getParent();
            if (parent == null) {
                return null;
            }
            Path summaryPath = parent.resolve(SUMMARY_FILE_NAME).normalize();
            return summaryPath.startsWith(parent) ? summaryPath : null;
        } catch (Exception ex) {
            logger.warn("resolve category classification summary path failed: {}", ex.getMessage());
            return null;
        }
    }

    private String buildStorageTaskPath(String videoId) {
        String normalizedVideoId = TaskManualCollectionRepository.normalizeTaskPath(videoId);
        if (normalizedVideoId.isEmpty()) {
            return "";
        }
        return "storage/" + normalizedVideoId;
    }

    private String firstNonBlank(String... candidates) {
        if (candidates == null) {
            return "";
        }
        for (String candidate : candidates) {
            if (candidate != null && !candidate.isBlank()) {
                return candidate;
            }
        }
        return "";
    }

    public record CategoryAssignment(
            String categoryPath,
            boolean archived,
            String archivedAt,
            boolean manualBinding
    ) {
    }

    private record Snapshot(
            Map<String, String> explicitBindings,
            Map<String, String> automaticBindings,
            Map<String, String> archivedTaskPaths,
            Map<String, String> archivedBindings
    ) {
    }
}
