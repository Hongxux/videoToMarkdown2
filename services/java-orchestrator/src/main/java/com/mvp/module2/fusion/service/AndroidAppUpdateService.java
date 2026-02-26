package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class AndroidAppUpdateService {
    private static final String DEFAULT_DOWNLOAD_ENDPOINT = "/api/mobile/app/update/apk";
    private static final String APK_EXTENSION = ".apk";

    @Value("${mobile.app.update.android.manifest-path:var/app-updates/android/latest.json}")
    private String androidManifestPath;

    @Value("${mobile.app.update.android.download-base-url:}")
    private String androidDownloadBaseUrl;

    @Value("${mobile.app.update.android.download-endpoint:/api/mobile/app/update/apk}")
    private String androidDownloadEndpoint;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public UpdateCheckPayload checkAndroidUpdate(
            Integer currentVersionCode,
            String currentVersionName,
            String requestBaseUrl
    ) throws IOException {
        boolean hasValidVersionCode = currentVersionCode != null && currentVersionCode > 0;
        boolean hasValidVersionName = StringUtils.hasText(currentVersionName);
        if (!hasValidVersionCode && !hasValidVersionName) {
            throw new IllegalArgumentException("versionCode 或 versionName 至少传一个");
        }

        AndroidReleaseManifest manifest = loadAndroidManifest();
        boolean hasUpdate = shouldUpgrade(currentVersionCode, currentVersionName, manifest);
        boolean forceUpdate = hasUpdate && shouldForceUpdate(currentVersionCode, manifest);
        String downloadUrl = resolveDownloadUrl(manifest, requestBaseUrl);

        return new UpdateCheckPayload(
                hasUpdate,
                forceUpdate,
                currentVersionCode,
                trimToEmpty(currentVersionName),
                manifest.versionCode,
                manifest.versionName,
                manifest.minSupportedVersionCode,
                manifest.publishedAt,
                manifest.releaseNotes,
                downloadUrl,
                manifest.sha256,
                manifest.fileSizeBytes
        );
    }

    public ResolvedApk resolveAndroidApk(Integer requestedVersionCode) throws IOException {
        AndroidReleaseManifest manifest = loadAndroidManifest();
        if (requestedVersionCode != null && requestedVersionCode > 0 && requestedVersionCode != manifest.versionCode) {
            throw new IllegalArgumentException("请求版本不存在，当前仅发布版本: " + manifest.versionCode);
        }
        if (!StringUtils.hasText(manifest.apkFile)) {
            throw new IllegalStateException("更新清单缺少 apkFile 字段");
        }

        Path apkPath = resolveApkPath(manifest.manifestPath, manifest.apkFile);
        if (!Files.exists(apkPath) || !Files.isRegularFile(apkPath)) {
            throw new FileNotFoundException("APK 文件不存在: " + apkPath);
        }

        String fileName = apkPath.getFileName().toString();
        if (!fileName.toLowerCase(Locale.ROOT).endsWith(APK_EXTENSION)) {
            throw new IllegalArgumentException("apkFile 必须是 .apk 文件");
        }

        long fileSize = Files.size(apkPath);
        return new ResolvedApk(
                apkPath,
                fileName,
                manifest.versionCode,
                manifest.versionName,
                manifest.sha256,
                fileSize
        );
    }

    private AndroidReleaseManifest loadAndroidManifest() throws IOException {
        Path manifestPath = Paths.get(androidManifestPath).toAbsolutePath().normalize();
        if (!Files.exists(manifestPath) || !Files.isRegularFile(manifestPath)) {
            throw new FileNotFoundException("更新清单不存在: " + manifestPath);
        }

        JsonNode root = objectMapper.readTree(Files.newInputStream(manifestPath));
        int versionCode = root.path("versionCode").asInt(-1);
        if (versionCode <= 0) {
            throw new IllegalArgumentException("更新清单 versionCode 必须是正整数");
        }

        String versionName = trimToEmpty(root.path("versionName").asText(""));
        if (!StringUtils.hasText(versionName)) {
            throw new IllegalArgumentException("更新清单 versionName 不能为空");
        }

        int minSupportedVersionCode = root.path("minSupportedVersionCode").asInt(versionCode);
        if (minSupportedVersionCode <= 0) {
            minSupportedVersionCode = versionCode;
        }
        boolean forceUpdate = root.path("forceUpdate").asBoolean(false);
        String apkFile = trimToEmpty(root.path("apkFile").asText(""));
        String explicitDownloadUrl = trimToEmpty(root.path("downloadUrl").asText(""));
        String sha256 = trimToEmpty(root.path("sha256").asText(""));
        long fileSizeBytes = root.path("fileSizeBytes").asLong(-1L);
        String publishedAt = trimToEmpty(root.path("publishedAt").asText(""));
        String releaseNotes = extractReleaseNotes(root.get("releaseNotes"));
        if (fileSizeBytes <= 0L && StringUtils.hasText(apkFile)) {
            try {
                Path apkPath = resolveApkPath(manifestPath, apkFile);
                if (Files.exists(apkPath) && Files.isRegularFile(apkPath)) {
                    fileSizeBytes = Files.size(apkPath);
                }
            } catch (Exception ex) {
                fileSizeBytes = -1L;
            }
        }

        return new AndroidReleaseManifest(
                manifestPath,
                versionCode,
                versionName,
                minSupportedVersionCode,
                forceUpdate,
                apkFile,
                explicitDownloadUrl,
                sha256,
                fileSizeBytes,
                publishedAt,
                releaseNotes
        );
    }

    private String extractReleaseNotes(JsonNode releaseNotesNode) {
        if (releaseNotesNode == null || releaseNotesNode.isNull()) {
            return "";
        }
        if (releaseNotesNode.isTextual()) {
            return trimToEmpty(releaseNotesNode.asText(""));
        }
        if (releaseNotesNode.isArray()) {
            List<String> lines = new ArrayList<>();
            for (JsonNode item : releaseNotesNode) {
                String line = trimToEmpty(item.asText(""));
                if (StringUtils.hasText(line)) {
                    lines.add(line);
                }
            }
            return String.join("\n", lines);
        }
        if (releaseNotesNode.isObject()) {
            Map<String, Object> payload = new LinkedHashMap<>();
            releaseNotesNode.fields().forEachRemaining(entry -> payload.put(entry.getKey(), entry.getValue().asText("")));
            try {
                return objectMapper.writeValueAsString(payload);
            } catch (Exception ex) {
                return "";
            }
        }
        return "";
    }

    private boolean shouldUpgrade(Integer currentVersionCode, String currentVersionName, AndroidReleaseManifest manifest) {
        if (currentVersionCode != null && currentVersionCode > 0) {
            return currentVersionCode < manifest.versionCode;
        }
        if (!StringUtils.hasText(currentVersionName)) {
            return false;
        }
        return compareVersionName(currentVersionName, manifest.versionName) < 0;
    }

    private boolean shouldForceUpdate(Integer currentVersionCode, AndroidReleaseManifest manifest) {
        if (currentVersionCode != null && currentVersionCode > 0
                && currentVersionCode < manifest.minSupportedVersionCode) {
            return true;
        }
        return manifest.forceUpdate;
    }

    private String resolveDownloadUrl(AndroidReleaseManifest manifest, String requestBaseUrl) {
        if (StringUtils.hasText(manifest.explicitDownloadUrl)) {
            return manifest.explicitDownloadUrl;
        }
        String baseUrl = StringUtils.hasText(androidDownloadBaseUrl) ? androidDownloadBaseUrl : requestBaseUrl;
        if (!StringUtils.hasText(baseUrl)) {
            return "";
        }

        String endpoint = normalizeEndpoint(androidDownloadEndpoint);
        return stripTrailingSlash(baseUrl)
                + endpoint
                + "?versionCode="
                + manifest.versionCode;
    }

    private Path resolveApkPath(Path manifestPath, String apkFile) {
        Path candidate = Paths.get(apkFile);
        if (candidate.isAbsolute()) {
            return candidate.normalize();
        }
        Path manifestDir = manifestPath.getParent() != null
                ? manifestPath.getParent().toAbsolutePath().normalize()
                : Paths.get(".").toAbsolutePath().normalize();
        Path resolved = manifestDir.resolve(candidate).normalize();
        if (!resolved.startsWith(manifestDir)) {
            throw new IllegalArgumentException("apkFile 路径越界");
        }
        return resolved;
    }

    static int compareVersionName(String leftVersion, String rightVersion) {
        String left = leftVersion == null ? "" : leftVersion.trim();
        String right = rightVersion == null ? "" : rightVersion.trim();
        if (left.equals(right)) {
            return 0;
        }
        String[] leftParts = left.split("[._-]");
        String[] rightParts = right.split("[._-]");
        int maxParts = Math.max(leftParts.length, rightParts.length);
        for (int index = 0; index < maxParts; index++) {
            String leftPart = index < leftParts.length ? leftParts[index] : "0";
            String rightPart = index < rightParts.length ? rightParts[index] : "0";
            Integer leftNumber = parseInteger(leftPart);
            Integer rightNumber = parseInteger(rightPart);
            int compare;
            if (leftNumber != null && rightNumber != null) {
                compare = Integer.compare(leftNumber, rightNumber);
            } else {
                compare = leftPart.compareToIgnoreCase(rightPart);
            }
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    private static Integer parseInteger(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        for (int idx = 0; idx < value.length(); idx++) {
            if (!Character.isDigit(value.charAt(idx))) {
                return null;
            }
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private String normalizeEndpoint(String rawEndpoint) {
        String endpoint = StringUtils.hasText(rawEndpoint) ? rawEndpoint.trim() : DEFAULT_DOWNLOAD_ENDPOINT;
        if (!endpoint.startsWith("/")) {
            endpoint = "/" + endpoint;
        }
        return endpoint;
    }

    private String stripTrailingSlash(String rawBaseUrl) {
        String base = rawBaseUrl.trim();
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base;
    }

    private String trimToEmpty(String value) {
        return value == null ? "" : value.trim();
    }

    private static final class AndroidReleaseManifest {
        private final Path manifestPath;
        private final int versionCode;
        private final String versionName;
        private final int minSupportedVersionCode;
        private final boolean forceUpdate;
        private final String apkFile;
        private final String explicitDownloadUrl;
        private final String sha256;
        private final long fileSizeBytes;
        private final String publishedAt;
        private final String releaseNotes;

        private AndroidReleaseManifest(
                Path manifestPath,
                int versionCode,
                String versionName,
                int minSupportedVersionCode,
                boolean forceUpdate,
                String apkFile,
                String explicitDownloadUrl,
                String sha256,
                long fileSizeBytes,
                String publishedAt,
                String releaseNotes
        ) {
            this.manifestPath = manifestPath;
            this.versionCode = versionCode;
            this.versionName = versionName;
            this.minSupportedVersionCode = minSupportedVersionCode;
            this.forceUpdate = forceUpdate;
            this.apkFile = apkFile;
            this.explicitDownloadUrl = explicitDownloadUrl;
            this.sha256 = sha256;
            this.fileSizeBytes = fileSizeBytes;
            this.publishedAt = publishedAt;
            this.releaseNotes = releaseNotes;
        }
    }

    public static final class UpdateCheckPayload {
        public final boolean hasUpdate;
        public final boolean forceUpdate;
        public final Integer currentVersionCode;
        public final String currentVersionName;
        public final int latestVersionCode;
        public final String latestVersionName;
        public final int minSupportedVersionCode;
        public final String publishedAt;
        public final String releaseNotes;
        public final String downloadUrl;
        public final String sha256;
        public final long fileSizeBytes;

        public UpdateCheckPayload(
                boolean hasUpdate,
                boolean forceUpdate,
                Integer currentVersionCode,
                String currentVersionName,
                int latestVersionCode,
                String latestVersionName,
                int minSupportedVersionCode,
                String publishedAt,
                String releaseNotes,
                String downloadUrl,
                String sha256,
                long fileSizeBytes
        ) {
            this.hasUpdate = hasUpdate;
            this.forceUpdate = forceUpdate;
            this.currentVersionCode = currentVersionCode;
            this.currentVersionName = currentVersionName;
            this.latestVersionCode = latestVersionCode;
            this.latestVersionName = latestVersionName;
            this.minSupportedVersionCode = minSupportedVersionCode;
            this.publishedAt = publishedAt;
            this.releaseNotes = releaseNotes;
            this.downloadUrl = downloadUrl;
            this.sha256 = sha256;
            this.fileSizeBytes = fileSizeBytes;
        }
    }

    public static final class ResolvedApk {
        public final Path path;
        public final String fileName;
        public final int versionCode;
        public final String versionName;
        public final String sha256;
        public final long fileSizeBytes;

        public ResolvedApk(
                Path path,
                String fileName,
                int versionCode,
                String versionName,
                String sha256,
                long fileSizeBytes
        ) {
            this.path = path;
            this.fileName = fileName;
            this.versionCode = versionCode;
            this.versionName = versionName;
            this.sha256 = sha256;
            this.fileSizeBytes = fileSizeBytes;
        }
    }
}
