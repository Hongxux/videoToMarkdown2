package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.service.AndroidAppUpdateAdminService;
import com.mvp.module2.fusion.service.AndroidAppUpdateAdminService.PublishResult;
import com.mvp.module2.fusion.service.AndroidAppUpdateAdminService.RollbackResult;
import com.mvp.module2.fusion.service.AndroidAppUpdateAdminService.UploadReleaseResult;
import com.mvp.module2.fusion.service.AndroidAppUpdateService;
import com.mvp.module2.fusion.service.AndroidAppUpdateService.ResolvedApk;
import com.mvp.module2.fusion.service.AndroidAppUpdateService.UpdateCheckPayload;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRange;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/mobile/app/update")
public class MobileAppUpdateController {
    private static final Logger logger = LoggerFactory.getLogger(MobileAppUpdateController.class);
    private static final MediaType APK_MEDIA_TYPE =
            MediaType.parseMediaType("application/vnd.android.package-archive");

    @Autowired
    private AndroidAppUpdateService androidAppUpdateService;

    @Autowired
    private AndroidAppUpdateAdminService androidAppUpdateAdminService;

    @Value("${mobile.app.update.android.admin-token:}")
    private String adminToken;

    @GetMapping("/check")
    public ResponseEntity<Map<String, Object>> checkAndroidUpdate(
            @RequestParam(value = "versionCode", required = false) Integer versionCode,
            @RequestParam(value = "versionName", required = false) String versionName,
            HttpServletRequest request
    ) {
        try {
            String baseUrl = resolveBaseUrl(request);
            UpdateCheckPayload payload = androidAppUpdateService.checkAndroidUpdate(versionCode, versionName, baseUrl);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("success", true);
            body.put("hasUpdate", payload.hasUpdate);
            body.put("forceUpdate", payload.forceUpdate);
            body.put("currentVersionCode", payload.currentVersionCode);
            body.put("currentVersionName", payload.currentVersionName);
            body.put("latestVersionCode", payload.latestVersionCode);
            body.put("latestVersionName", payload.latestVersionName);
            body.put("minSupportedVersionCode", payload.minSupportedVersionCode);
            body.put("publishedAt", payload.publishedAt);
            body.put("releaseNotes", payload.releaseNotes);
            body.put("downloadUrl", payload.downloadUrl);
            body.put("sha256", payload.sha256);
            body.put("fileSizeBytes", payload.fileSizeBytes);
            return ResponseEntity.ok(body);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("check android update failed: versionCode={} versionName={} err={}",
                    versionCode, versionName, ex.getMessage());
            return ResponseEntity.status(503).body(Map.of("success", false, "message", "update manifest not ready"));
        } catch (Exception ex) {
            logger.error("check android update failed unexpectedly", ex);
            return ResponseEntity.status(500).body(Map.of("success", false, "message", "check update failed"));
        }
    }

    @GetMapping("/apk")
    public ResponseEntity<?> downloadAndroidApk(
            @RequestParam(value = "versionCode", required = false) Integer versionCode,
            @RequestHeader(value = HttpHeaders.RANGE, required = false) String rangeHeader
    ) {
        try {
            ResolvedApk apk = androidAppUpdateService.resolveAndroidApk(versionCode);
            long fileSizeBytes = apk.fileSizeBytes > 0 ? apk.fileSizeBytes : Files.size(apk.path);
            if (StringUtils.hasText(rangeHeader)) {
                try {
                    List<HttpRange> ranges = HttpRange.parseRanges(rangeHeader);
                    if (!ranges.isEmpty()) {
                        HttpRange range = ranges.get(0);
                        long start = range.getRangeStart(fileSizeBytes);
                        long end = range.getRangeEnd(fileSizeBytes);
                        long regionLength = end - start + 1L;
                        InputStream fileStream = Files.newInputStream(apk.path);
                        skipFully(fileStream, start);
                        InputStream bounded = new FilterInputStream(fileStream) {
                            long remaining = regionLength;

                            @Override
                            public int read() throws IOException {
                                if (remaining <= 0L) {
                                    return -1;
                                }
                                int value = super.read();
                                if (value >= 0) {
                                    remaining--;
                                }
                                return value;
                            }

                            @Override
                            public int read(byte[] buffer, int offset, int length) throws IOException {
                                if (remaining <= 0L) {
                                    return -1;
                                }
                                int nextLength = (int) Math.min((long) length, remaining);
                                int readCount = super.read(buffer, offset, nextLength);
                                if (readCount > 0) {
                                    remaining -= readCount;
                                }
                                return readCount;
                            }
                        };
                        Resource rangeResource = new InputStreamResource(bounded);
                        return ResponseEntity.status(HttpStatus.PARTIAL_CONTENT)
                                .contentType(APK_MEDIA_TYPE)
                                .contentLength(regionLength)
                                .header(HttpHeaders.ACCEPT_RANGES, "bytes")
                                .header(HttpHeaders.CONTENT_RANGE, "bytes " + start + "-" + end + "/" + fileSizeBytes)
                                .header(HttpHeaders.CACHE_CONTROL, "no-store")
                                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + apk.fileName + "\"")
                                .body(rangeResource);
                    }
                } catch (Exception ex) {
                    logger.warn("range request processing failed, fallback to full apk response: versionCode={} range={} err={}",
                            versionCode, rangeHeader, ex.getMessage());
                }
            }
            Resource resource = new InputStreamResource(Files.newInputStream(apk.path));
            return ResponseEntity.ok()
                    .contentType(APK_MEDIA_TYPE)
                    .contentLength(fileSizeBytes)
                    .header(HttpHeaders.ACCEPT_RANGES, "bytes")
                    .header(HttpHeaders.CACHE_CONTROL, "no-store")
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + apk.fileName + "\"")
                    .body(resource);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", ex.getMessage()));
        } catch (IllegalStateException ex) {
            return ResponseEntity.status(503).body(Map.of("success", false, "message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("download android apk failed: versionCode={} err={}", versionCode, ex.getMessage());
            return ResponseEntity.status(404).body(Map.of("success", false, "message", "apk file not found"));
        } catch (Exception ex) {
            logger.error("download android apk failed unexpectedly", ex);
            return ResponseEntity.status(500).body(Map.of("success", false, "message", "download apk failed"));
        }
    }

    private void skipFully(InputStream stream, long bytesToSkip) throws IOException {
        long remaining = Math.max(0L, bytesToSkip);
        while (remaining > 0L) {
            long skipped = stream.skip(remaining);
            if (skipped > 0L) {
                remaining -= skipped;
                continue;
            }
            if (stream.read() < 0) {
                throw new IOException("unexpected end of stream while skipping bytes");
            }
            remaining--;
        }
    }

    @PostMapping(value = "/admin/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> uploadAndroidRelease(
            @RequestHeader(value = "X-Update-Admin-Token", required = false) String tokenHeader,
            @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorizationHeader,
            @RequestPart("apk") MultipartFile apkFile,
            @RequestParam("versionCode") Integer versionCode,
            @RequestParam("versionName") String versionName,
            @RequestParam(value = "minSupportedVersionCode", required = false) Integer minSupportedVersionCode,
            @RequestParam(value = "forceUpdate", defaultValue = "false") boolean forceUpdate,
            @RequestParam(value = "releaseNotes", required = false) String releaseNotes,
            @RequestParam(value = "publish", defaultValue = "true") boolean publish
    ) {
        AuthCheckResult auth = checkAdminAuth(tokenHeader, authorizationHeader);
        if (!auth.authorized) {
            return ResponseEntity.status(auth.status).body(Map.of("success", false, "message", auth.message));
        }
        try {
            UploadReleaseResult result = androidAppUpdateAdminService.uploadRelease(
                    apkFile,
                    versionCode != null ? versionCode : -1,
                    versionName,
                    minSupportedVersionCode,
                    forceUpdate,
                    releaseNotes,
                    publish
            );
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("versionCode", result.versionCode);
            payload.put("versionName", result.versionName);
            payload.put("apkRelativePath", result.apkRelativePath);
            payload.put("releaseManifestPath", result.releaseManifestPath);
            payload.put("fileSizeBytes", result.fileSizeBytes);
            payload.put("sha256", result.sha256);
            payload.put("published", result.published);
            if (result.publishResult != null) {
                payload.put("latestVersionCode", result.publishResult.versionCode);
                payload.put("latestVersionName", result.publishResult.versionName);
                payload.put("previousVersionCode", result.publishResult.previousVersionCode);
                payload.put("latestManifestPath", result.publishResult.latestManifestPath);
            }
            return ResponseEntity.ok(payload);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("upload android release failed: versionCode={} versionName={} err={}",
                    versionCode, versionName, ex.getMessage());
            return ResponseEntity.status(503).body(Map.of("success", false, "message", "upload release failed"));
        } catch (Exception ex) {
            logger.error("upload android release failed unexpectedly", ex);
            return ResponseEntity.status(500).body(Map.of("success", false, "message", "upload release failed"));
        }
    }

    @PostMapping("/admin/publish")
    public ResponseEntity<Map<String, Object>> publishAndroidRelease(
            @RequestHeader(value = "X-Update-Admin-Token", required = false) String tokenHeader,
            @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorizationHeader,
            @RequestParam("versionCode") Integer versionCode
    ) {
        AuthCheckResult auth = checkAdminAuth(tokenHeader, authorizationHeader);
        if (!auth.authorized) {
            return ResponseEntity.status(auth.status).body(Map.of("success", false, "message", auth.message));
        }
        try {
            PublishResult result = androidAppUpdateAdminService.publishRelease(versionCode != null ? versionCode : -1);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("versionCode", result.versionCode);
            payload.put("versionName", result.versionName);
            payload.put("previousVersionCode", result.previousVersionCode);
            payload.put("latestManifestPath", result.latestManifestPath);
            return ResponseEntity.ok(payload);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("publish android release failed: versionCode={} err={}", versionCode, ex.getMessage());
            return ResponseEntity.status(503).body(Map.of("success", false, "message", "publish release failed"));
        } catch (Exception ex) {
            logger.error("publish android release failed unexpectedly", ex);
            return ResponseEntity.status(500).body(Map.of("success", false, "message", "publish release failed"));
        }
    }

    @PostMapping("/admin/rollback")
    public ResponseEntity<Map<String, Object>> rollbackAndroidRelease(
            @RequestHeader(value = "X-Update-Admin-Token", required = false) String tokenHeader,
            @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorizationHeader,
            @RequestParam(value = "targetVersionCode", required = false) Integer targetVersionCode
    ) {
        AuthCheckResult auth = checkAdminAuth(tokenHeader, authorizationHeader);
        if (!auth.authorized) {
            return ResponseEntity.status(auth.status).body(Map.of("success", false, "message", auth.message));
        }
        try {
            RollbackResult result = androidAppUpdateAdminService.rollbackRelease(targetVersionCode);
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("success", true);
            payload.put("rolledBackFromVersionCode", result.rolledBackFromVersionCode);
            payload.put("rolledBackToVersionCode", result.rolledBackToVersionCode);
            payload.put("latestManifestPath", result.latestManifestPath);
            return ResponseEntity.ok(payload);
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", ex.getMessage()));
        } catch (IllegalStateException ex) {
            return ResponseEntity.status(409).body(Map.of("success", false, "message", ex.getMessage()));
        } catch (IOException ex) {
            logger.warn("rollback android release failed: targetVersionCode={} err={}",
                    targetVersionCode, ex.getMessage());
            return ResponseEntity.status(503).body(Map.of("success", false, "message", "rollback release failed"));
        } catch (Exception ex) {
            logger.error("rollback android release failed unexpectedly", ex);
            return ResponseEntity.status(500).body(Map.of("success", false, "message", "rollback release failed"));
        }
    }

    private String resolveBaseUrl(HttpServletRequest request) {
        String forwardedBaseUrl = resolveForwardedBaseUrl(request);
        if (StringUtils.hasText(forwardedBaseUrl)) {
            return forwardedBaseUrl;
        }
        try {
            return ServletUriComponentsBuilder.fromCurrentContextPath().build().toUriString();
        } catch (Exception ex) {
            if (request == null) {
                return "";
            }
            String scheme = request.getScheme() != null ? request.getScheme() : "http";
            String host = request.getServerName() != null ? request.getServerName() : "localhost";
            int port = request.getServerPort();
            boolean defaultPort = ("http".equalsIgnoreCase(scheme) && port == 80)
                    || ("https".equalsIgnoreCase(scheme) && port == 443);
            return defaultPort ? scheme + "://" + host : scheme + "://" + host + ":" + port;
        }
    }

    private String resolveForwardedBaseUrl(HttpServletRequest request) {
        if (request == null) {
            return "";
        }
        String forwardedProto = firstForwardedToken(request.getHeader("X-Forwarded-Proto"));
        String forwardedHost = firstForwardedToken(request.getHeader("X-Forwarded-Host"));
        String forwardedPort = firstForwardedToken(request.getHeader("X-Forwarded-Port"));
        if (!StringUtils.hasText(forwardedProto) || !StringUtils.hasText(forwardedHost)) {
            return "";
        }
        String scheme = forwardedProto.trim();
        String host = forwardedHost.trim();
        if (host.isEmpty()) {
            return "";
        }
        if (!host.contains(":") && StringUtils.hasText(forwardedPort)) {
            String portText = forwardedPort.trim();
            if (!portText.isEmpty()) {
                try {
                    int port = Integer.parseInt(portText);
                    boolean defaultPort = ("http".equalsIgnoreCase(scheme) && port == 80)
                            || ("https".equalsIgnoreCase(scheme) && port == 443);
                    if (!defaultPort) {
                        host = host + ":" + port;
                    }
                } catch (NumberFormatException ignored) {
                    // 非法端口直接忽略，回退到 host 原值
                }
            }
        }
        return scheme + "://" + host;
    }

    private String firstForwardedToken(String headerValue) {
        if (!StringUtils.hasText(headerValue)) {
            return "";
        }
        String[] segments = headerValue.split(",");
        if (segments.length == 0) {
            return "";
        }
        return segments[0].trim();
    }

    private AuthCheckResult checkAdminAuth(String tokenHeader, String authorizationHeader) {
        if (!StringUtils.hasText(adminToken)) {
            return AuthCheckResult.unavailable("admin token not configured");
        }
        String providedToken = extractProvidedToken(tokenHeader, authorizationHeader);
        if (!StringUtils.hasText(providedToken)) {
            return AuthCheckResult.unauthorized("missing admin token");
        }
        if (!constantTimeEquals(adminToken.trim(), providedToken.trim())) {
            return AuthCheckResult.unauthorized("invalid admin token");
        }
        return AuthCheckResult.authorized();
    }

    private String extractProvidedToken(String tokenHeader, String authorizationHeader) {
        if (StringUtils.hasText(tokenHeader)) {
            return tokenHeader;
        }
        if (!StringUtils.hasText(authorizationHeader)) {
            return "";
        }
        String rawHeader = authorizationHeader.trim();
        if (rawHeader.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return rawHeader.substring(7).trim();
        }
        return rawHeader;
    }

    private boolean constantTimeEquals(String expected, String provided) {
        byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);
        byte[] providedBytes = provided.getBytes(StandardCharsets.UTF_8);
        return MessageDigest.isEqual(expectedBytes, providedBytes);
    }

    private static final class AuthCheckResult {
        private final boolean authorized;
        private final int status;
        private final String message;

        private AuthCheckResult(boolean authorized, int status, String message) {
            this.authorized = authorized;
            this.status = status;
            this.message = message;
        }

        private static AuthCheckResult authorized() {
            return new AuthCheckResult(true, 200, "");
        }

        private static AuthCheckResult unauthorized(String message) {
            return new AuthCheckResult(false, 401, message);
        }

        private static AuthCheckResult unavailable(String message) {
            return new AuthCheckResult(false, 503, message);
        }
    }
}
