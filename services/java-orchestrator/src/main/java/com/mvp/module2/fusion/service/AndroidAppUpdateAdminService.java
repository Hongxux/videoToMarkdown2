package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class AndroidAppUpdateAdminService {
    private static final String APK_EXTENSION = ".apk";
    private static final Pattern UNSAFE_FILENAME_CHARS = Pattern.compile("[^A-Za-z0-9._-]");

    @Value("${mobile.app.update.android.manifest-path:var/app-updates/android/latest.json}")
    private String androidLatestManifestPath;

    @Value("${mobile.app.update.android.releases-dir:var/app-updates/android/releases}")
    private String androidReleasesDir;

    @Value("${mobile.app.update.android.release-apk-subdir:apk}")
    private String androidReleaseApkSubdir;

    @Value("${mobile.app.update.android.publish-history-path:var/app-updates/android/publish-history.json}")
    private String androidPublishHistoryPath;

    @Autowired
    private FileTransferService fileTransferService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public synchronized UploadReleaseResult uploadRelease(
            MultipartFile apkFile,
            int versionCode,
            String versionName,
            Integer minSupportedVersionCode,
            boolean forceUpdate,
            String releaseNotes,
            boolean publishNow
    ) throws IOException {
        validateUploadRequest(apkFile, versionCode, versionName);
        Path latestManifestPath = latestManifestPath();
        Path latestManifestDir = latestManifestPath.getParent() != null
                ? latestManifestPath.getParent().toAbsolutePath().normalize()
                : Paths.get(".").toAbsolutePath().normalize();
        Path releasesDir = releasesDirPath();
        Path releaseApkDir = releasesDir.resolve(safeSubPath(androidReleaseApkSubdir)).normalize();
        Path releaseManifestPath = releasesDir.resolve(versionCode + ".json").normalize();
        if (!releaseManifestPath.startsWith(releasesDir) || !releaseApkDir.startsWith(releasesDir)) {
            throw new IllegalArgumentException("发布目录配置非法");
        }

        Files.createDirectories(releasesDir);
        Files.createDirectories(releaseApkDir);

        String normalizedVersionName = versionName.trim();
        int minSupported = normalizeMinSupported(versionCode, minSupportedVersionCode);
        String safeVersionName = sanitizeFileStem(normalizedVersionName);
        String fileName = "videoToMarkdown-" + versionCode + "-" + safeVersionName + ".apk";
        Path targetApkPath = releaseApkDir.resolve(fileName).normalize();
        if (!targetApkPath.startsWith(releaseApkDir)) {
            throw new IllegalArgumentException("APK 存储路径越界");
        }

        fileTransferService.persistMultipartToPath(releaseApkDir, targetApkPath, apkFile);
        long fileSizeBytes = Files.size(targetApkPath);
        String sha256 = sha256Hex(targetApkPath);
        String publishedAt = Instant.now().toString();
        String normalizedNotes = normalizeReleaseNotes(releaseNotes);
        String relativeApkPath = toUnixRelativePath(latestManifestDir, targetApkPath);

        ObjectNode manifestNode = objectMapper.createObjectNode();
        manifestNode.put("versionCode", versionCode);
        manifestNode.put("versionName", normalizedVersionName);
        manifestNode.put("minSupportedVersionCode", minSupported);
        manifestNode.put("forceUpdate", forceUpdate);
        manifestNode.put("apkFile", relativeApkPath);
        manifestNode.put("sha256", sha256);
        manifestNode.put("fileSizeBytes", fileSizeBytes);
        manifestNode.put("publishedAt", publishedAt);
        if (StringUtils.hasText(normalizedNotes)) {
            manifestNode.put("releaseNotes", normalizedNotes);
        } else {
            manifestNode.put("releaseNotes", "");
        }

        writeJsonAtomically(releaseManifestPath, manifestNode);

        PublishResult publishResult = null;
        if (publishNow) {
            publishResult = publishRelease(versionCode);
        }

        return new UploadReleaseResult(
                versionCode,
                normalizedVersionName,
                relativeApkPath,
                releaseManifestPath.toString(),
                fileSizeBytes,
                sha256,
                publishResult != null,
                publishResult
        );
    }

    public synchronized PublishResult publishRelease(int versionCode) throws IOException {
        if (versionCode <= 0) {
            throw new IllegalArgumentException("versionCode 必须是正整数");
        }
        Path latestManifestPath = latestManifestPath();
        Path latestManifestDir = latestManifestPath.getParent() != null
                ? latestManifestPath.getParent().toAbsolutePath().normalize()
                : Paths.get(".").toAbsolutePath().normalize();
        Path releaseManifestPath = releasesDirPath().resolve(versionCode + ".json").normalize();
        if (!Files.exists(releaseManifestPath) || !Files.isRegularFile(releaseManifestPath)) {
            throw new IllegalArgumentException("发布版本不存在: " + versionCode);
        }

        ObjectNode releaseNode = readReleaseManifest(releaseManifestPath);
        validateReleaseNode(releaseNode, latestManifestDir);
        int releaseVersionCode = releaseNode.path("versionCode").asInt(-1);
        if (releaseVersionCode != versionCode) {
            throw new IllegalArgumentException("发布文件 versionCode 不匹配: " + releaseVersionCode);
        }

        Integer previousVersionCode = readCurrentLatestVersionCode(latestManifestPath);
        writeJsonAtomically(latestManifestPath, releaseNode);
        recordPublishHistory(versionCode, previousVersionCode);

        return new PublishResult(
                versionCode,
                releaseNode.path("versionName").asText(""),
                previousVersionCode,
                latestManifestPath.toString()
        );
    }

    public synchronized RollbackResult rollbackRelease(Integer targetVersionCode) throws IOException {
        Path latestManifestPath = latestManifestPath();
        Integer currentVersionCode = readCurrentLatestVersionCode(latestManifestPath);
        if (currentVersionCode == null || currentVersionCode <= 0) {
            throw new IllegalStateException("当前 latest.json 不存在有效版本，无法回滚");
        }

        int rollbackTarget = targetVersionCode != null && targetVersionCode > 0
                ? targetVersionCode
                : selectPreviousVersionFromHistory(currentVersionCode);
        if (rollbackTarget == currentVersionCode) {
            throw new IllegalArgumentException("回滚目标与当前版本一致");
        }

        PublishResult publishResult = publishRelease(rollbackTarget);
        return new RollbackResult(
                currentVersionCode,
                rollbackTarget,
                publishResult.latestManifestPath
        );
    }

    private void validateUploadRequest(MultipartFile apkFile, int versionCode, String versionName) {
        if (apkFile == null || apkFile.isEmpty()) {
            throw new IllegalArgumentException("apk 文件不能为空");
        }
        if (versionCode <= 0) {
            throw new IllegalArgumentException("versionCode 必须是正整数");
        }
        if (!StringUtils.hasText(versionName)) {
            throw new IllegalArgumentException("versionName 不能为空");
        }
        String originalName = apkFile.getOriginalFilename() != null
                ? apkFile.getOriginalFilename().trim()
                : "";
        if (!originalName.toLowerCase(Locale.ROOT).endsWith(APK_EXTENSION)) {
            throw new IllegalArgumentException("上传文件必须是 .apk");
        }
    }

    private int normalizeMinSupported(int versionCode, Integer minSupportedVersionCode) {
        if (minSupportedVersionCode == null || minSupportedVersionCode <= 0) {
            return versionCode;
        }
        return Math.min(versionCode, minSupportedVersionCode);
    }

    private String normalizeReleaseNotes(String releaseNotes) {
        if (!StringUtils.hasText(releaseNotes)) {
            return "";
        }
        return releaseNotes.trim();
    }

    private Path latestManifestPath() {
        return Paths.get(androidLatestManifestPath).toAbsolutePath().normalize();
    }

    private Path releasesDirPath() {
        return Paths.get(androidReleasesDir).toAbsolutePath().normalize();
    }

    private String safeSubPath(String rawSubPath) {
        String normalized = StringUtils.hasText(rawSubPath) ? rawSubPath.trim() : "apk";
        normalized = normalized.replace('\\', '/');
        while (normalized.startsWith("/")) {
            normalized = normalized.substring(1);
        }
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        if (normalized.isBlank()) {
            return "apk";
        }
        return normalized;
    }

    private String sanitizeFileStem(String rawValue) {
        String value = rawValue == null ? "" : rawValue.trim();
        String sanitized = UNSAFE_FILENAME_CHARS.matcher(value).replaceAll("_");
        while (sanitized.contains("__")) {
            sanitized = sanitized.replace("__", "_");
        }
        sanitized = sanitized.replaceAll("^[_\\.\\-]+", "").replaceAll("[_\\.\\-]+$", "");
        if (sanitized.isEmpty()) {
            sanitized = "release";
        }
        return sanitized;
    }

    private String sha256Hex(Path filePath) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream inputStream = Files.newInputStream(filePath)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = inputStream.read(buffer)) > 0) {
                    digest.update(buffer, 0, read);
                }
            }
            byte[] bytes = digest.digest();
            StringBuilder builder = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (Exception ex) {
            throw new IOException("计算 SHA-256 失败", ex);
        }
    }

    private String toUnixRelativePath(Path baseDir, Path targetPath) {
        Path normalizedBase = baseDir.toAbsolutePath().normalize();
        Path normalizedTarget = targetPath.toAbsolutePath().normalize();
        if (!normalizedTarget.startsWith(normalizedBase)) {
            throw new IllegalArgumentException("APK 路径不在更新目录内");
        }
        return normalizedBase.relativize(normalizedTarget).toString().replace('\\', '/');
    }

    private ObjectNode readReleaseManifest(Path releaseManifestPath) throws IOException {
        JsonNode root = objectMapper.readTree(Files.newInputStream(releaseManifestPath));
        if (!(root instanceof ObjectNode objectNode)) {
            throw new IllegalArgumentException("发布文件结构非法: " + releaseManifestPath);
        }
        return objectNode;
    }

    private void validateReleaseNode(ObjectNode releaseNode, Path latestManifestDir) {
        int versionCode = releaseNode.path("versionCode").asInt(-1);
        String versionName = releaseNode.path("versionName").asText("");
        String apkFile = releaseNode.path("apkFile").asText("");
        if (versionCode <= 0 || !StringUtils.hasText(versionName) || !StringUtils.hasText(apkFile)) {
            throw new IllegalArgumentException("发布文件缺少必要字段");
        }
        Path apkPath = latestManifestDir.resolve(apkFile).normalize();
        if (!apkPath.startsWith(latestManifestDir)) {
            throw new IllegalArgumentException("发布文件 apkFile 路径越界");
        }
        if (!Files.exists(apkPath) || !Files.isRegularFile(apkPath)) {
            throw new IllegalArgumentException("发布文件引用的 APK 不存在: " + apkPath);
        }
    }

    private Integer readCurrentLatestVersionCode(Path latestManifestPath) throws IOException {
        if (!Files.exists(latestManifestPath) || !Files.isRegularFile(latestManifestPath)) {
            return null;
        }
        JsonNode root = objectMapper.readTree(Files.newInputStream(latestManifestPath));
        int versionCode = root.path("versionCode").asInt(-1);
        return versionCode > 0 ? versionCode : null;
    }

    private void recordPublishHistory(int newVersionCode, Integer previousVersionCode) throws IOException {
        Path historyPath = Paths.get(androidPublishHistoryPath).toAbsolutePath().normalize();
        if (historyPath.getParent() != null) {
            Files.createDirectories(historyPath.getParent());
        }
        ArrayNode entries = objectMapper.createArrayNode();
        if (Files.exists(historyPath) && Files.isRegularFile(historyPath)) {
            JsonNode oldRoot = objectMapper.readTree(Files.newInputStream(historyPath));
            JsonNode oldEntries = oldRoot.path("entries");
            if (oldEntries.isArray()) {
                oldEntries.forEach(entries::add);
            }
        }

        ObjectNode entry = objectMapper.createObjectNode();
        entry.put("timestamp", Instant.now().toString());
        entry.put("versionCode", newVersionCode);
        if (previousVersionCode != null && previousVersionCode > 0) {
            entry.put("previousVersionCode", previousVersionCode);
        } else {
            entry.putNull("previousVersionCode");
        }
        entries.add(entry);

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("schema", "android-update-history-v1");
        payload.set("entries", entries);
        writeJsonAtomically(historyPath, payload);
    }

    private int selectPreviousVersionFromHistory(int currentVersionCode) throws IOException {
        Path historyPath = Paths.get(androidPublishHistoryPath).toAbsolutePath().normalize();
        if (!Files.exists(historyPath) || !Files.isRegularFile(historyPath)) {
            throw new IllegalStateException("发布历史不存在，请显式指定回滚版本");
        }
        JsonNode root = objectMapper.readTree(Files.newInputStream(historyPath));
        JsonNode entries = root.path("entries");
        if (!entries.isArray() || entries.isEmpty()) {
            throw new IllegalStateException("发布历史为空，请显式指定回滚版本");
        }

        List<Integer> history = new ArrayList<>();
        for (JsonNode item : entries) {
            int versionCode = item.path("versionCode").asInt(-1);
            if (versionCode > 0) {
                history.add(versionCode);
            }
        }
        for (int idx = history.size() - 1; idx >= 0; idx--) {
            int candidate = history.get(idx);
            if (candidate != currentVersionCode) {
                return candidate;
            }
        }
        throw new IllegalStateException("发布历史中没有可回滚的旧版本");
    }

    private void writeJsonAtomically(Path target, JsonNode root) throws IOException {
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        Path tempPath = Files.createTempFile(
                target.getParent() != null ? target.getParent() : Paths.get("."),
                target.getFileName().toString(),
                ".tmp"
        );
        try {
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
            Files.writeString(tempPath, json, StandardCharsets.UTF_8);
            Files.move(tempPath, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (Exception ex) {
            Files.deleteIfExists(tempPath);
            throw ex;
        }
    }

    public static final class UploadReleaseResult {
        public final int versionCode;
        public final String versionName;
        public final String apkRelativePath;
        public final String releaseManifestPath;
        public final long fileSizeBytes;
        public final String sha256;
        public final boolean published;
        public final PublishResult publishResult;

        public UploadReleaseResult(
                int versionCode,
                String versionName,
                String apkRelativePath,
                String releaseManifestPath,
                long fileSizeBytes,
                String sha256,
                boolean published,
                PublishResult publishResult
        ) {
            this.versionCode = versionCode;
            this.versionName = versionName;
            this.apkRelativePath = apkRelativePath;
            this.releaseManifestPath = releaseManifestPath;
            this.fileSizeBytes = fileSizeBytes;
            this.sha256 = sha256;
            this.published = published;
            this.publishResult = publishResult;
        }
    }

    public static final class PublishResult {
        public final int versionCode;
        public final String versionName;
        public final Integer previousVersionCode;
        public final String latestManifestPath;

        public PublishResult(
                int versionCode,
                String versionName,
                Integer previousVersionCode,
                String latestManifestPath
        ) {
            this.versionCode = versionCode;
            this.versionName = versionName;
            this.previousVersionCode = previousVersionCode;
            this.latestManifestPath = latestManifestPath;
        }
    }

    public static final class RollbackResult {
        public final int rolledBackFromVersionCode;
        public final int rolledBackToVersionCode;
        public final String latestManifestPath;

        public RollbackResult(
                int rolledBackFromVersionCode,
                int rolledBackToVersionCode,
                String latestManifestPath
        ) {
            this.rolledBackFromVersionCode = rolledBackFromVersionCode;
            this.rolledBackToVersionCode = rolledBackToVersionCode;
            this.latestManifestPath = latestManifestPath;
        }
    }
}
