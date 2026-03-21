package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileNotFoundException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 统一读取任务目录 video_meta.json，避免 Controller/Service 各自实现解析分支。
 */
@Service
public class VideoMetaService {

    private static final Logger logger = LoggerFactory.getLogger(VideoMetaService.class);
    private final ObjectMapper objectMapper;

    public VideoMetaService() {
        this(new ObjectMapper());
    }

    @Autowired
    public VideoMetaService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
    }

    public VideoMetaSnapshot read(Path taskRoot) {
        ObjectNode root = readOrCreateNode(taskRoot);
        String title = trimToNull(root.path("title").asText(null));
        String domain = trimToNull(root.path("domain").asText(null));
        String mainTopic = trimToNull(root.path("main_topic").asText(null));
        if (mainTopic == null) {
            mainTopic = trimToNull(root.path("mainTopic").asText(null));
        }
        return new VideoMetaSnapshot(title, domain, mainTopic);
    }

    public String readTitle(Path taskRoot) {
        return read(taskRoot).title;
    }

    public ObjectNode readOrCreateNode(Path taskRoot) {
        Path metaPath = resolveVideoMetaPath(taskRoot);
        if (metaPath == null) {
            return objectMapper.createObjectNode();
        }
        return readExistingNode(metaPath, true);
    }

    private ObjectNode readExistingNode(Path metaPath, boolean allowRetry) {
        if (metaPath == null || !Files.isRegularFile(metaPath)) {
            return objectMapper.createObjectNode();
        }
        try {
            byte[] rawBytes = Files.readAllBytes(metaPath);
            if (rawBytes.length == 0) {
                return objectMapper.createObjectNode();
            }
            JsonNode loaded = objectMapper.readTree(rawBytes);
            if (loaded instanceof ObjectNode objectNode) {
                return objectNode;
            }
            return objectMapper.createObjectNode();
        } catch (NoSuchFileException | FileNotFoundException missingDuringRead) {
            if (allowRetry) {
                return readExistingNode(metaPath, false);
            }
            logger.debug(
                    "video metadata disappeared during read, treat as empty node: path={} type={}",
                    metaPath,
                    missingDuringRead.getClass().getSimpleName()
            );
            return objectMapper.createObjectNode();
        } catch (Exception ex) {
            logger.warn(
                    "read video metadata failed: path={} type={} err={}",
                    metaPath,
                    ex.getClass().getSimpleName(),
                    describeExceptionMessage(ex)
            );
            return objectMapper.createObjectNode();
        }
    }

    public boolean writeTocMetadata(Path taskRoot, String contentType, List<Map<String, Object>> bookSectionTree) {
        Path metaPath = resolveVideoMetaPath(taskRoot);
        if (metaPath == null) {
            return false;
        }
        Path normalizedRoot = taskRoot.toAbsolutePath().normalize();
        Path tmpPath = normalizedRoot.resolve("video_meta.json.tmp").normalize();
        if (!tmpPath.startsWith(normalizedRoot)) {
            return false;
        }
        try {
            Files.createDirectories(normalizedRoot);
            ObjectNode root = readOrCreateNode(normalizedRoot);
            String normalizedContentType = trimToNull(contentType);
            if (normalizedContentType == null) {
                root.remove("contentType");
            } else {
                root.put("contentType", normalizedContentType);
            }
            List<Map<String, Object>> safeTree = bookSectionTree != null ? bookSectionTree : Collections.emptyList();
            if (safeTree.isEmpty()) {
                root.remove("bookSectionTree");
            } else {
                root.set("bookSectionTree", objectMapper.valueToTree(safeTree));
            }
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmpPath.toFile(), root);
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
            }
            logger.warn(
                    "write video toc metadata failed: path={} type={} err={}",
                    metaPath,
                    ex.getClass().getSimpleName(),
                    describeExceptionMessage(ex)
            );
            return false;
        }
    }

    private Path resolveVideoMetaPath(Path taskRoot) {
        if (taskRoot == null) {
            return null;
        }
        try {
            Path normalizedRoot = taskRoot.toAbsolutePath().normalize();
            Path metaPath = normalizedRoot.resolve("video_meta.json").normalize();
            if (!metaPath.startsWith(normalizedRoot)) {
                return null;
            }
            return metaPath;
        } catch (Exception ignored) {
            return null;
        }
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String describeExceptionMessage(Exception ex) {
        if (ex == null) {
            return "<empty>";
        }
        String message = ex.getMessage();
        if (message == null || message.isBlank()) {
            return "<empty>";
        }
        return message.trim();
    }

    public static class VideoMetaSnapshot {
        public final String title;
        public final String domain;
        public final String mainTopic;

        public VideoMetaSnapshot(String title, String domain, String mainTopic) {
            this.title = title;
            this.domain = domain;
            this.mainTopic = mainTopic;
        }
    }
}