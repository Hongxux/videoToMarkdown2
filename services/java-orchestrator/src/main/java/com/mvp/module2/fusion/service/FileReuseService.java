package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PreDestroy;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

@Service
public class FileReuseService {

    private static final Logger logger = LoggerFactory.getLogger(FileReuseService.class);
    private static final Pattern MD5_HEX_PATTERN = Pattern.compile("^[a-f0-9]{32}$");
    private static final Pattern FILE_EXT_PATTERN = Pattern.compile("^\\.[a-z0-9]{1,16}$");

    private final FileReuseRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService digestExecutor = Executors.newFixedThreadPool(2, runnable -> {
        Thread worker = new Thread(runnable);
        worker.setName("file-reuse-digest-" + UUID.randomUUID());
        worker.setDaemon(true);
        return worker;
    });

    public FileReuseService(FileReuseRepository repository) {
        this.repository = repository;
    }

    @PreDestroy
    public void shutdown() {
        digestExecutor.shutdown();
    }

    public Optional<FileFingerprint> normalizeFingerprint(String rawMd5, String rawExt, String fallbackFileName) {
        String normalizedMd5 = normalizeMd5(rawMd5);
        if (normalizedMd5 == null) {
            return Optional.empty();
        }
        String normalizedExt = normalizeFileExt(rawExt, fallbackFileName);
        if (normalizedExt == null) {
            return Optional.empty();
        }
        return Optional.of(new FileFingerprint(normalizedMd5, normalizedExt));
    }

    public Optional<Path> findReusablePath(FileFingerprint fingerprint) {
        if (fingerprint == null) {
            return Optional.empty();
        }
        Optional<FileReuseRepository.FileMetadataRecord> rowOpt =
                repository.findFileByMd5AndExt(fingerprint.md5(), fingerprint.fileExt());
        if (rowOpt.isEmpty()) {
            return Optional.empty();
        }
        String filePathText = rowOpt.get().filePath;
        if (filePathText == null || filePathText.isBlank()) {
            return Optional.empty();
        }
        Path normalizedPath;
        try {
            normalizedPath = Paths.get(filePathText).toAbsolutePath().normalize();
        } catch (Exception ex) {
            logger.warn("invalid reuse path in file metadata: path={} err={}", filePathText, ex.getMessage());
            return Optional.empty();
        }
        if (!Files.isRegularFile(normalizedPath)) {
            logger.info("reuse candidate path missing on disk: md5={} ext={} path={}",
                    fingerprint.md5(), fingerprint.fileExt(), normalizedPath);
            return Optional.empty();
        }
        return Optional.of(normalizedPath);
    }

    public Optional<FileFingerprint> findFingerprintByPath(String filePath) {
        String normalizedPath = normalizePathText(filePath);
        if (normalizedPath == null) {
            return Optional.empty();
        }
        Optional<FileReuseRepository.FileMetadataRecord> rowOpt = repository.findFileByPath(normalizedPath);
        if (rowOpt.isEmpty()) {
            return Optional.empty();
        }
        FileReuseRepository.FileMetadataRecord row = rowOpt.get();
        return normalizeFingerprint(row.fileMd5, row.fileExt, row.originalFileName);
    }

    public void recordUploadedFile(FileFingerprint fingerprint, Path filePath, Long fileSize, String originalFileName) {
        if (fingerprint == null || filePath == null) {
            return;
        }
        String normalizedPath = normalizePathText(filePath.toString());
        if (normalizedPath == null) {
            return;
        }
        repository.upsertFileMetadata(
                fingerprint.md5(),
                fingerprint.fileExt(),
                normalizedPath,
                fileSize,
                originalFileName
        );
    }

    public void recordUploadedFileAsync(Path filePath, String originalFileName, Long fileSize) {
        if (filePath == null) {
            return;
        }
        Path normalizedPath = filePath.toAbsolutePath().normalize();
        digestExecutor.submit(() -> {
            try {
                String md5 = computeFileMd5(normalizedPath);
                String ext = normalizeFileExt(null, originalFileName != null ? originalFileName : normalizedPath.getFileName().toString());
                if (md5 == null || ext == null) {
                    return;
                }
                repository.upsertFileMetadata(md5, ext, normalizedPath.toString(), fileSize, originalFileName);
            } catch (Exception ex) {
                logger.warn("async file md5 compute failed: path={} err={}", normalizedPath, ex.getMessage());
            }
        });
    }

    public Optional<Map<String, Object>> findProbePayload(FileFingerprint fingerprint) {
        if (fingerprint == null) {
            return Optional.empty();
        }
        Optional<String> payloadOpt = repository.findProbePayloadByMd5AndExt(fingerprint.md5(), fingerprint.fileExt());
        if (payloadOpt.isEmpty() || payloadOpt.get() == null || payloadOpt.get().isBlank()) {
            return Optional.empty();
        }
        try {
            Map<String, Object> payload = objectMapper.readValue(
                    payloadOpt.get(),
                    new TypeReference<LinkedHashMap<String, Object>>() {}
            );
            if (payload == null || payload.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(payload);
        } catch (Exception ex) {
            logger.warn("parse probe payload cache failed: md5={} ext={} err={}",
                    fingerprint.md5(), fingerprint.fileExt(), ex.getMessage());
            return Optional.empty();
        }
    }

    public void recordProbePayload(FileFingerprint fingerprint, Map<String, Object> payload) {
        if (fingerprint == null || payload == null || payload.isEmpty()) {
            return;
        }
        try {
            String payloadJson = objectMapper.writeValueAsString(payload);
            repository.upsertProbePayload(fingerprint.md5(), fingerprint.fileExt(), payloadJson);
        } catch (Exception ex) {
            logger.warn("serialize probe payload failed: md5={} ext={} err={}",
                    fingerprint.md5(), fingerprint.fileExt(), ex.getMessage());
        }
    }

    public String normalizeFileExt(String rawExt, String fallbackFileName) {
        String candidate = rawExt != null ? rawExt.trim() : "";
        if (candidate.isEmpty() && fallbackFileName != null) {
            String fallback = fallbackFileName.trim();
            int dotIndex = fallback.lastIndexOf('.');
            if (dotIndex >= 0 && dotIndex < fallback.length() - 1) {
                candidate = fallback.substring(dotIndex);
            }
        }
        if (candidate.isEmpty()) {
            return null;
        }
        if (!candidate.startsWith(".")) {
            candidate = "." + candidate;
        }
        String normalized = candidate.toLowerCase(Locale.ROOT);
        if (!FILE_EXT_PATTERN.matcher(normalized).matches()) {
            return null;
        }
        return normalized;
    }

    private String normalizeMd5(String rawMd5) {
        if (rawMd5 == null) {
            return null;
        }
        String normalized = rawMd5.trim().toLowerCase(Locale.ROOT);
        if (!MD5_HEX_PATTERN.matcher(normalized).matches()) {
            return null;
        }
        return normalized;
    }

    private String normalizePathText(String rawPath) {
        if (rawPath == null || rawPath.isBlank()) {
            return null;
        }
        try {
            return Paths.get(rawPath).toAbsolutePath().normalize().toString();
        } catch (Exception ex) {
            return null;
        }
    }

    private String computeFileMd5(Path filePath) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        byte[] buffer = new byte[1024 * 1024];
        try (InputStream inputStream = Files.newInputStream(filePath)) {
            while (true) {
                int read = inputStream.read(buffer);
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    continue;
                }
                digest.update(buffer, 0, read);
            }
        }
        byte[] bytes = digest.digest();
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte one : bytes) {
            builder.append(String.format("%02x", one));
        }
        return builder.toString();
    }

    public static class FileFingerprint {
        private final String md5;
        private final String fileExt;

        public FileFingerprint(String md5, String fileExt) {
            this.md5 = md5;
            this.fileExt = fileExt;
        }

        public String md5() {
            return md5;
        }

        public String fileExt() {
            return fileExt;
        }
    }
}
