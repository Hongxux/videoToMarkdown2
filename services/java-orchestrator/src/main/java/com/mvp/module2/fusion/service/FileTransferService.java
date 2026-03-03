package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

@Service
public class FileTransferService {
    private static final String CHUNK_UPLOAD_ROOT_DIR = ".chunk_uploads";
    private static final String CHUNK_META_FILE = "meta.json";
    private static final String CHUNK_PART_PREFIX = "chunk_";
    private static final String CHUNK_PART_SUFFIX = ".part";

    private final AsyncTaskExecutor taskExecutor;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Semaphore transferLimiter;
    private final int copyBufferBytes;

    public FileTransferService(
            @Qualifier("taskExecutor") AsyncTaskExecutor taskExecutor,
            @Value("${file.transfer.max-concurrent:8}") int maxConcurrentTransfers,
            @Value("${file.transfer.copy-buffer-bytes:65536}") int copyBufferBytes
    ) {
        this.taskExecutor = taskExecutor;
        this.transferLimiter = new Semaphore(Math.max(1, maxConcurrentTransfers));
        this.copyBufferBytes = Math.max(4096, copyBufferBytes);
    }

    public Path resolveTransferRoot(String transferRootDir) throws IOException {
        Path transferRootPath = Paths.get(transferRootDir).toAbsolutePath().normalize();
        Files.createDirectories(transferRootPath);
        return transferRootPath;
    }

    public Path resolveUniqueTargetPath(Path transferRootPath, String safeFileName) throws IOException {
        String uniquePrefix = Instant.now().toEpochMilli() + "_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        Path targetPath = transferRootPath.resolve(uniquePrefix + "_" + safeFileName).toAbsolutePath().normalize();
        ensurePathWithinRoot(targetPath, transferRootPath, "illegal upload path");
        return targetPath;
    }

    public Path persistMultipartToPath(Path targetPath, MultipartFile multipartFile) throws IOException {
        Path normalizedTarget = targetPath.toAbsolutePath().normalize();
        Path parentPath = normalizedTarget.getParent();
        if (parentPath != null) {
            Files.createDirectories(parentPath);
        }
        withTransferPermit(() -> streamMultipartToPath(multipartFile, normalizedTarget, null, false));
        return normalizedTarget;
    }

    public Path persistMultipartToPath(Path transferRootPath, Path targetPath, MultipartFile multipartFile) throws IOException {
        Path normalizedTarget = targetPath.toAbsolutePath().normalize();
        ensurePathWithinRoot(normalizedTarget, transferRootPath, "illegal target path");
        return persistMultipartToPath(normalizedTarget, multipartFile);
    }

    public CompletableFuture<Path> persistMultipartToPathAsync(
            Path transferRootPath,
            Path targetPath,
            MultipartFile multipartFile
    ) {
        return supplyIoAsync(() -> persistMultipartToPath(transferRootPath, targetPath, multipartFile));
    }

    public Path persistMultipartWithUniqueName(String transferRootDir, String safeFileName, MultipartFile multipartFile) throws IOException {
        Path transferRootPath = resolveTransferRoot(transferRootDir);
        return persistMultipartWithUniqueName(transferRootPath, safeFileName, multipartFile);
    }

    public CompletableFuture<Path> persistMultipartWithUniqueNameAsync(
            String transferRootDir,
            String safeFileName,
            MultipartFile multipartFile
    ) {
        return supplyIoAsync(() -> persistMultipartWithUniqueName(transferRootDir, safeFileName, multipartFile));
    }

    public Path persistMultipartWithUniqueName(Path transferRootPath, String safeFileName, MultipartFile multipartFile) throws IOException {
        Path targetPath = resolveUniqueTargetPath(transferRootPath, safeFileName);
        return persistMultipartToPath(transferRootPath, targetPath, multipartFile);
    }

    public CompletableFuture<Path> persistMultipartWithUniqueNameAsync(
            Path transferRootPath,
            String safeFileName,
            MultipartFile multipartFile
    ) {
        return supplyIoAsync(() -> persistMultipartWithUniqueName(transferRootPath, safeFileName, multipartFile));
    }

    public CompletableFuture<List<Path>> persistMultipartBatchAsync(Path transferRootPath, List<BatchTransferItem> transferItems) {
        return supplyIoAsync(() -> {
            List<Path> persistedPaths = new ArrayList<>();
            if (transferItems == null || transferItems.isEmpty()) {
                return persistedPaths;
            }
            for (BatchTransferItem item : transferItems) {
                if (item == null || item.multipartFile == null || item.targetPath == null) {
                    continue;
                }
                Path persisted = persistMultipartToPath(transferRootPath, item.targetPath, item.multipartFile);
                persistedPaths.add(persisted);
            }
            return persistedPaths;
        });
    }

    public ChunkWriteResult writeChunk(
            Path transferRootPath,
            String uploadId,
            String safeFileName,
            int totalChunks,
            long totalFileSize,
            int chunkIndex,
            MultipartFile chunkFile
    ) throws IOException {
        return writeChunk(
                transferRootPath,
                uploadId,
                safeFileName,
                totalChunks,
                totalFileSize,
                chunkIndex,
                chunkFile,
                null
        );
    }

    public ChunkWriteResult writeChunk(
            Path transferRootPath,
            String uploadId,
            String safeFileName,
            int totalChunks,
            long totalFileSize,
            int chunkIndex,
            MultipartFile chunkFile,
            String expectedChunkSha256
    ) throws IOException {
        if (totalChunks <= 0 || chunkIndex < 0 || chunkIndex >= totalChunks) {
            throw new IllegalArgumentException("invalid chunk index or totalChunks");
        }
        String normalizedExpectedSha256 = normalizeSha256Hex(expectedChunkSha256);
        Path sessionDir = resolveChunkSessionDir(transferRootPath, uploadId, true);
        ChunkSessionMeta meta = readChunkMeta(sessionDir);
        if (meta == null) {
            meta = new ChunkSessionMeta(uploadId, safeFileName, totalChunks, totalFileSize, System.currentTimeMillis());
            writeChunkMeta(sessionDir, meta);
        } else if (!meta.matches(uploadId, safeFileName, totalChunks, totalFileSize)) {
            throw new IllegalArgumentException("chunk session metadata mismatch");
        }
        Path partPath = resolveChunkPartPath(sessionDir, chunkIndex);
        StreamingCopyResult copyResult = withTransferPermit(
                () -> streamMultipartToPath(chunkFile, partPath, normalizedExpectedSha256, true)
        );
        List<Integer> uploadedChunks = scanUploadedChunkIndexes(sessionDir);
        return new ChunkWriteResult(meta, chunkIndex, uploadedChunks, copyResult.sha256Hex, copyResult.totalBytes);
    }

    public CompletableFuture<ChunkWriteResult> writeChunkAsync(
            Path transferRootPath,
            String uploadId,
            String safeFileName,
            int totalChunks,
            long totalFileSize,
            int chunkIndex,
            MultipartFile chunkFile
    ) {
        return writeChunkAsync(
                transferRootPath,
                uploadId,
                safeFileName,
                totalChunks,
                totalFileSize,
                chunkIndex,
                chunkFile,
                null
        );
    }

    public CompletableFuture<ChunkWriteResult> writeChunkAsync(
            Path transferRootPath,
            String uploadId,
            String safeFileName,
            int totalChunks,
            long totalFileSize,
            int chunkIndex,
            MultipartFile chunkFile,
            String expectedChunkSha256
    ) {
        return supplyIoAsync(() -> writeChunk(
                transferRootPath,
                uploadId,
                safeFileName,
                totalChunks,
                totalFileSize,
                chunkIndex,
                chunkFile,
                expectedChunkSha256
        ));
    }

    public ChunkSessionStatus readChunkStatus(Path transferRootPath, String uploadId) throws IOException {
        Path sessionDir = resolveChunkSessionDir(transferRootPath, uploadId, false);
        if (!Files.isDirectory(sessionDir)) {
            return ChunkSessionStatus.missing(uploadId);
        }
        ChunkSessionMeta meta = readChunkMeta(sessionDir);
        List<Integer> uploadedChunks = scanUploadedChunkIndexes(sessionDir);
        return ChunkSessionStatus.found(uploadId, uploadedChunks, meta);
    }

    public CompletableFuture<ChunkSessionStatus> readChunkStatusAsync(Path transferRootPath, String uploadId) {
        return supplyIoAsync(() -> readChunkStatus(transferRootPath, uploadId));
    }

    public ChunkMergeResult mergeChunkSession(Path transferRootPath, String uploadId) throws IOException {
        Path sessionDir = resolveChunkSessionDir(transferRootPath, uploadId, false);
        if (!Files.isDirectory(sessionDir)) {
            throw new IllegalArgumentException("chunk session not found");
        }
        ChunkSessionMeta meta = readChunkMeta(sessionDir);
        if (meta == null || meta.safeFileName == null || meta.safeFileName.isBlank() || meta.totalChunks <= 0) {
            throw new IllegalArgumentException("chunk session metadata missing");
        }
        for (int index = 0; index < meta.totalChunks; index += 1) {
            Path partPath = resolveChunkPartPath(sessionDir, index);
            if (!Files.exists(partPath) || !Files.isRegularFile(partPath)) {
                throw new IllegalArgumentException("chunk missing at index " + index);
            }
        }

        Path mergedPath = resolveUniqueTargetPath(transferRootPath, meta.safeFileName);
        long mergedBytes = withTransferPermit(() -> mergeChunkFiles(sessionDir, meta.totalChunks, mergedPath));
        if (meta.totalFileSize >= 0 && mergedBytes != meta.totalFileSize) {
            Files.deleteIfExists(mergedPath);
            throw new IOException("merged file size mismatch");
        }
        return new ChunkMergeResult(mergedPath, meta, mergedBytes);
    }

    public CompletableFuture<ChunkMergeResult> mergeChunkSessionAsync(Path transferRootPath, String uploadId) {
        return supplyIoAsync(() -> mergeChunkSession(transferRootPath, uploadId));
    }

    public void cleanupChunkSessionQuietly(Path transferRootPath, String uploadId) {
        try {
            Path sessionDir = resolveChunkSessionDir(transferRootPath, uploadId, false);
            cleanupPathRecursivelyQuietly(sessionDir);
        } catch (Exception ignored) {
            // Ignore cleanup errors to avoid affecting main flow.
        }
    }

    public void cleanupPathRecursivelyQuietly(Path dirPath) {
        if (dirPath == null || !Files.exists(dirPath)) {
            return;
        }
        try {
            Files.walkFileTree(dirPath, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.deleteIfExists(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.deleteIfExists(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException ignored) {
            // Ignore cleanup errors to avoid affecting main flow.
        }
    }

    private Path resolveChunkSessionDir(Path transferRootPath, String uploadId, boolean createIfMissing) throws IOException {
        Path chunkRootPath = transferRootPath.resolve(CHUNK_UPLOAD_ROOT_DIR).toAbsolutePath().normalize();
        if (createIfMissing) {
            Files.createDirectories(chunkRootPath);
        }
        Path sessionDir = chunkRootPath.resolve(uploadId).toAbsolutePath().normalize();
        ensurePathWithinRoot(sessionDir, chunkRootPath, "illegal chunk session path");
        if (createIfMissing) {
            Files.createDirectories(sessionDir);
        }
        return sessionDir;
    }

    private Path resolveChunkMetaPath(Path sessionDir) {
        return sessionDir.resolve(CHUNK_META_FILE).toAbsolutePath().normalize();
    }

    private Path resolveChunkPartPath(Path sessionDir, int chunkIndex) {
        return sessionDir.resolve(CHUNK_PART_PREFIX + chunkIndex + CHUNK_PART_SUFFIX).toAbsolutePath().normalize();
    }

    private ChunkSessionMeta readChunkMeta(Path sessionDir) throws IOException {
        Path metaPath = resolveChunkMetaPath(sessionDir);
        if (!Files.exists(metaPath) || !Files.isRegularFile(metaPath)) {
            return null;
        }
        ChunkUploadMetaPayload payload = objectMapper.readValue(metaPath.toFile(), ChunkUploadMetaPayload.class);
        if (payload == null) {
            return null;
        }
        return new ChunkSessionMeta(
                payload.uploadId,
                payload.safeFileName,
                payload.totalChunks,
                payload.totalFileSize,
                payload.createdAtEpochMs
        );
    }

    private void writeChunkMeta(Path sessionDir, ChunkSessionMeta meta) throws IOException {
        Path metaPath = resolveChunkMetaPath(sessionDir);
        Path tempPath = sessionDir.resolve(CHUNK_META_FILE + ".tmp");
        ChunkUploadMetaPayload payload = new ChunkUploadMetaPayload();
        payload.uploadId = meta.uploadId;
        payload.safeFileName = meta.safeFileName;
        payload.totalChunks = meta.totalChunks;
        payload.totalFileSize = meta.totalFileSize;
        payload.createdAtEpochMs = meta.createdAtEpochMs;
        objectMapper.writeValue(tempPath.toFile(), payload);
        try {
            Files.move(tempPath, metaPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(tempPath, metaPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private List<Integer> scanUploadedChunkIndexes(Path sessionDir) throws IOException {
        List<Integer> uploaded = new ArrayList<>();
        if (!Files.isDirectory(sessionDir)) {
            return uploaded;
        }
        try (Stream<Path> stream = Files.list(sessionDir)) {
            stream.forEach(path -> {
                String fileName = path.getFileName().toString();
                if (!fileName.startsWith(CHUNK_PART_PREFIX) || !fileName.endsWith(CHUNK_PART_SUFFIX)) {
                    return;
                }
                String indexText = fileName.substring(
                        CHUNK_PART_PREFIX.length(),
                        fileName.length() - CHUNK_PART_SUFFIX.length()
                );
                try {
                    int chunkIndex = Integer.parseInt(indexText);
                    if (chunkIndex >= 0) {
                        uploaded.add(chunkIndex);
                    }
                } catch (NumberFormatException ignored) {
                    // Skip unexpected file names to avoid polluting chunk resume status.
                }
            });
        }
        uploaded.sort(Integer::compareTo);
        return uploaded;
    }

    private long mergeChunkFiles(Path sessionDir, int totalChunks, Path mergedPath) throws IOException {
        long mergedBytes = 0L;
        try (OutputStream outputStream = Files.newOutputStream(
                mergedPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
        )) {
            byte[] buffer = new byte[copyBufferBytes];
            for (int index = 0; index < totalChunks; index += 1) {
                Path partPath = resolveChunkPartPath(sessionDir, index);
                try (InputStream inputStream = Files.newInputStream(partPath, StandardOpenOption.READ)) {
                    int read;
                    while ((read = inputStream.read(buffer)) >= 0) {
                        if (read == 0) {
                            continue;
                        }
                        outputStream.write(buffer, 0, read);
                        mergedBytes += read;
                    }
                }
            }
        } catch (Exception ex) {
            Files.deleteIfExists(mergedPath);
            throw ex;
        }
        return mergedBytes;
    }

    private StreamingCopyResult streamMultipartToPath(
            MultipartFile multipartFile,
            Path targetPath,
            String expectedSha256Hex,
            boolean calculateSha256
    ) throws IOException {
        Path normalizedTarget = targetPath.toAbsolutePath().normalize();
        Path parentPath = normalizedTarget.getParent();
        if (parentPath != null) {
            Files.createDirectories(parentPath);
        }
        MessageDigest digest = calculateSha256 ? buildSha256Digest() : null;
        long totalBytes = 0L;
        try (InputStream inputStream = multipartFile.getInputStream();
             OutputStream outputStream = Files.newOutputStream(
                     normalizedTarget,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.TRUNCATE_EXISTING,
                     StandardOpenOption.WRITE
             )) {
            byte[] buffer = new byte[copyBufferBytes];
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
                if (read == 0) {
                    continue;
                }
                outputStream.write(buffer, 0, read);
                totalBytes += read;
                if (digest != null) {
                    digest.update(buffer, 0, read);
                }
            }
        } catch (Exception ex) {
            Files.deleteIfExists(normalizedTarget);
            throw ex;
        }
        String actualSha256Hex = digest != null ? toHex(digest.digest()) : "";
        if (StringUtils.hasText(expectedSha256Hex) && !expectedSha256Hex.equalsIgnoreCase(actualSha256Hex)) {
            Files.deleteIfExists(normalizedTarget);
            throw new IllegalArgumentException("chunk checksum mismatch");
        }
        return new StreamingCopyResult(totalBytes, actualSha256Hex);
    }

    private String normalizeSha256Hex(String rawSha256Hex) {
        if (!StringUtils.hasText(rawSha256Hex)) {
            return null;
        }
        String normalized = rawSha256Hex.trim().toLowerCase(Locale.ROOT);
        if (!normalized.matches("^[0-9a-f]{64}$")) {
            throw new IllegalArgumentException("invalid chunk checksum");
        }
        return normalized;
    }

    private MessageDigest buildSha256Digest() throws IOException {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Exception ex) {
            throw new IOException("sha-256 digest unavailable", ex);
        }
    }

    private String toHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * 2);
        for (byte one : bytes) {
            builder.append(String.format("%02x", one));
        }
        return builder.toString();
    }

    private <T> T withTransferPermit(IoSupplier<T> supplier) throws IOException {
        boolean acquired = false;
        try {
            transferLimiter.acquire();
            acquired = true;
            return supplier.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IOException("transfer interrupted", ex);
        } finally {
            if (acquired) {
                transferLimiter.release();
            }
        }
    }

    private void ensurePathWithinRoot(Path targetPath, Path rootPath, String errorMessage) throws IOException {
        Path normalizedRoot = rootPath.toAbsolutePath().normalize();
        Path normalizedTarget = targetPath.toAbsolutePath().normalize();
        if (!normalizedTarget.startsWith(normalizedRoot)) {
            throw new IOException(errorMessage);
        }
    }

    private <T> CompletableFuture<T> supplyIoAsync(IoSupplier<T> supplier) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return supplier.get();
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        }, taskExecutor);
    }

    @FunctionalInterface
    private interface IoSupplier<T> {
        T get() throws IOException;
    }

    private static final class ChunkUploadMetaPayload {
        public String uploadId;
        public String safeFileName;
        public int totalChunks;
        public long totalFileSize;
        public long createdAtEpochMs;
    }

    private static final class StreamingCopyResult {
        private final long totalBytes;
        private final String sha256Hex;

        private StreamingCopyResult(long totalBytes, String sha256Hex) {
            this.totalBytes = totalBytes;
            this.sha256Hex = sha256Hex != null ? sha256Hex : "";
        }
    }

    public static final class BatchTransferItem {
        public final Path targetPath;
        public final MultipartFile multipartFile;

        public BatchTransferItem(Path targetPath, MultipartFile multipartFile) {
            this.targetPath = targetPath;
            this.multipartFile = multipartFile;
        }
    }

    public static final class ChunkSessionMeta {
        public final String uploadId;
        public final String safeFileName;
        public final int totalChunks;
        public final long totalFileSize;
        public final long createdAtEpochMs;

        public ChunkSessionMeta(String uploadId, String safeFileName, int totalChunks, long totalFileSize, long createdAtEpochMs) {
            this.uploadId = uploadId != null ? uploadId : "";
            this.safeFileName = safeFileName != null ? safeFileName : "";
            this.totalChunks = totalChunks;
            this.totalFileSize = totalFileSize;
            this.createdAtEpochMs = createdAtEpochMs;
        }

        private boolean matches(String expectedUploadId, String expectedSafeFileName, int expectedTotalChunks, long expectedTotalFileSize) {
            boolean fileSizeCompatible = (this.totalFileSize < 0 || expectedTotalFileSize < 0 || this.totalFileSize == expectedTotalFileSize);
            return this.uploadId.equals(expectedUploadId)
                    && this.safeFileName.equals(expectedSafeFileName)
                    && this.totalChunks == expectedTotalChunks
                    && fileSizeCompatible;
        }
    }

    public static final class ChunkWriteResult {
        public final ChunkSessionMeta meta;
        public final int chunkIndex;
        public final List<Integer> uploadedChunks;
        public final String chunkSha256;
        public final long chunkSizeBytes;

        public ChunkWriteResult(
                ChunkSessionMeta meta,
                int chunkIndex,
                List<Integer> uploadedChunks,
                String chunkSha256,
                long chunkSizeBytes
        ) {
            this.meta = meta;
            this.chunkIndex = chunkIndex;
            this.uploadedChunks = uploadedChunks == null ? List.of() : List.copyOf(uploadedChunks);
            this.chunkSha256 = chunkSha256 != null ? chunkSha256 : "";
            this.chunkSizeBytes = chunkSizeBytes;
        }
    }

    public static final class ChunkSessionStatus {
        public final boolean exists;
        public final String uploadId;
        public final List<Integer> uploadedChunks;
        public final ChunkSessionMeta meta;

        private ChunkSessionStatus(boolean exists, String uploadId, List<Integer> uploadedChunks, ChunkSessionMeta meta) {
            this.exists = exists;
            this.uploadId = uploadId != null ? uploadId : "";
            this.uploadedChunks = uploadedChunks == null ? List.of() : List.copyOf(uploadedChunks);
            this.meta = meta;
        }

        public static ChunkSessionStatus missing(String uploadId) {
            return new ChunkSessionStatus(false, uploadId, List.of(), null);
        }

        public static ChunkSessionStatus found(String uploadId, List<Integer> uploadedChunks, ChunkSessionMeta meta) {
            return new ChunkSessionStatus(true, uploadId, uploadedChunks, meta);
        }
    }

    public static final class ChunkMergeResult {
        public final Path mergedPath;
        public final ChunkSessionMeta meta;
        public final long mergedBytes;

        public ChunkMergeResult(Path mergedPath, ChunkSessionMeta meta, long mergedBytes) {
            this.mergedPath = mergedPath;
            this.meta = meta;
            this.mergedBytes = mergedBytes;
        }
    }
}
