package com.mvp.module2.fusion.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class FileReuseRepository {

    private final JdbcTemplate jdbcTemplate;

    public FileReuseRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<FileMetadataRecord> findFileByMd5AndExt(String fileMd5, String fileExt) {
        String normalizedMd5 = normalizeText(fileMd5);
        String normalizedExt = normalizeText(fileExt);
        if (normalizedMd5.isEmpty() || normalizedExt.isEmpty()) {
            return Optional.empty();
        }
        List<FileMetadataRecord> rows = jdbcTemplate.query(
                """
                SELECT file_md5, file_ext, file_path, file_size, original_file_name, created_at, updated_at
                FROM file_metadata
                WHERE file_md5 = ? AND file_ext = ?
                LIMIT 1
                """,
                (rs, rowNum) -> new FileMetadataRecord(
                        rs.getString("file_md5"),
                        rs.getString("file_ext"),
                        rs.getString("file_path"),
                        rs.getObject("file_size", Long.class),
                        rs.getString("original_file_name"),
                        rs.getString("created_at"),
                        rs.getString("updated_at")
                ),
                normalizedMd5,
                normalizedExt
        );
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rows.get(0));
    }

    public Optional<FileMetadataRecord> findFileByPath(String filePath) {
        String normalizedPath = normalizeText(filePath);
        if (normalizedPath.isEmpty()) {
            return Optional.empty();
        }
        List<FileMetadataRecord> rows = jdbcTemplate.query(
                """
                SELECT file_md5, file_ext, file_path, file_size, original_file_name, created_at, updated_at
                FROM file_metadata
                WHERE file_path = ?
                LIMIT 1
                """,
                (rs, rowNum) -> new FileMetadataRecord(
                        rs.getString("file_md5"),
                        rs.getString("file_ext"),
                        rs.getString("file_path"),
                        rs.getObject("file_size", Long.class),
                        rs.getString("original_file_name"),
                        rs.getString("created_at"),
                        rs.getString("updated_at")
                ),
                normalizedPath
        );
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rows.get(0));
    }

    @Transactional
    public void upsertFileMetadata(
            String fileMd5,
            String fileExt,
            String filePath,
            Long fileSize,
            String originalFileName
    ) {
        String normalizedMd5 = normalizeText(fileMd5);
        String normalizedExt = normalizeText(fileExt);
        String normalizedPath = normalizeText(filePath);
        if (normalizedMd5.isEmpty() || normalizedExt.isEmpty() || normalizedPath.isEmpty()) {
            return;
        }
        Long normalizedSize = fileSize != null && fileSize >= 0 ? fileSize : null;
        String now = Instant.now().toString();
        jdbcTemplate.update(
                """
                INSERT INTO file_metadata (
                    file_md5, file_ext, file_path, file_size, original_file_name, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_md5, file_ext) DO UPDATE SET
                    file_path = excluded.file_path,
                    file_size = excluded.file_size,
                    original_file_name = excluded.original_file_name,
                    updated_at = excluded.updated_at
                """,
                normalizedMd5,
                normalizedExt,
                normalizedPath,
                normalizedSize,
                normalizeText(originalFileName),
                now,
                now
        );
    }

    public Optional<String> findProbePayloadByMd5AndExt(String fileMd5, String fileExt) {
        String normalizedMd5 = normalizeText(fileMd5);
        String normalizedExt = normalizeText(fileExt);
        if (normalizedMd5.isEmpty() || normalizedExt.isEmpty()) {
            return Optional.empty();
        }
        List<String> rows = jdbcTemplate.query(
                """
                SELECT probe_payload
                FROM file_probe_cache
                WHERE file_md5 = ? AND file_ext = ?
                LIMIT 1
                """,
                (rs, rowNum) -> rs.getString("probe_payload"),
                normalizedMd5,
                normalizedExt
        );
        if (rows.isEmpty()) {
            return Optional.empty();
        }
        return Optional.ofNullable(rows.get(0));
    }

    @Transactional
    public void upsertProbePayload(String fileMd5, String fileExt, String probePayload) {
        String normalizedMd5 = normalizeText(fileMd5);
        String normalizedExt = normalizeText(fileExt);
        String normalizedPayload = normalizeText(probePayload);
        if (normalizedMd5.isEmpty() || normalizedExt.isEmpty() || normalizedPayload.isEmpty()) {
            return;
        }
        String now = Instant.now().toString();
        jdbcTemplate.update(
                """
                INSERT INTO file_probe_cache (
                    file_md5, file_ext, probe_payload, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(file_md5, file_ext) DO UPDATE SET
                    probe_payload = excluded.probe_payload,
                    updated_at = excluded.updated_at
                """,
                normalizedMd5,
                normalizedExt,
                normalizedPayload,
                now,
                now
        );
    }

    private String normalizeText(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    public static class FileMetadataRecord {
        public final String fileMd5;
        public final String fileExt;
        public final String filePath;
        public final Long fileSize;
        public final String originalFileName;
        public final String createdAt;
        public final String updatedAt;

        public FileMetadataRecord(
                String fileMd5,
                String fileExt,
                String filePath,
                Long fileSize,
                String originalFileName,
                String createdAt,
                String updatedAt
        ) {
            this.fileMd5 = fileMd5;
            this.fileExt = fileExt;
            this.filePath = filePath;
            this.fileSize = fileSize;
            this.originalFileName = originalFileName;
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
        }
    }
}
