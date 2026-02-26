package com.mvp.module2.fusion.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AndroidAppUpdateServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void shouldDetectUpgradeByVersionCodeAndMarkForceUpdate() throws Exception {
        Path manifestDir = Files.createDirectories(tempDir.resolve("var/app-updates/android"));
        Path manifestPath = manifestDir.resolve("latest.json");
        Path apkPath = manifestDir.resolve("videoToMarkdown-1.3.0.apk");
        Files.write(apkPath, "apk-binary".getBytes(StandardCharsets.UTF_8));

        String manifest = """
                {
                  "versionCode": 130,
                  "versionName": "1.3.0",
                  "minSupportedVersionCode": 120,
                  "forceUpdate": false,
                  "apkFile": "videoToMarkdown-1.3.0.apk",
                  "sha256": "sha-example",
                  "fileSizeBytes": 1024,
                  "publishedAt": "2026-02-24T00:00:00Z",
                  "releaseNotes": ["fix A", "fix B"]
                }
                """;
        Files.writeString(manifestPath, manifest, StandardCharsets.UTF_8);

        AndroidAppUpdateService service = new AndroidAppUpdateService();
        injectField(service, "androidManifestPath", manifestPath.toString());
        injectField(service, "androidDownloadBaseUrl", "https://api.example.com");
        injectField(service, "androidDownloadEndpoint", "/api/mobile/app/update/apk");

        AndroidAppUpdateService.UpdateCheckPayload payload =
                service.checkAndroidUpdate(110, "1.1.0", "http://localhost:8080");

        assertTrue(payload.hasUpdate);
        assertTrue(payload.forceUpdate);
        assertEquals(130, payload.latestVersionCode);
        assertEquals("1.3.0", payload.latestVersionName);
        assertEquals("https://api.example.com/api/mobile/app/update/apk?versionCode=130", payload.downloadUrl);

        AndroidAppUpdateService.ResolvedApk apk = service.resolveAndroidApk(130);
        assertEquals("videoToMarkdown-1.3.0.apk", apk.fileName);
        assertEquals(130, apk.versionCode);
    }

    @Test
    void shouldFallbackToVersionNameCompareWhenVersionCodeMissing() throws Exception {
        Path manifestDir = Files.createDirectories(tempDir.resolve("feed"));
        Path manifestPath = manifestDir.resolve("latest.json");
        Path apkPath = manifestDir.resolve("videoToMarkdown-2.0.0.apk");
        Files.write(apkPath, "apk-binary".getBytes(StandardCharsets.UTF_8));

        String manifest = """
                {
                  "versionCode": 200,
                  "versionName": "2.0.0",
                  "apkFile": "videoToMarkdown-2.0.0.apk"
                }
                """;
        Files.writeString(manifestPath, manifest, StandardCharsets.UTF_8);

        AndroidAppUpdateService service = new AndroidAppUpdateService();
        injectField(service, "androidManifestPath", manifestPath.toString());
        injectField(service, "androidDownloadBaseUrl", "");
        injectField(service, "androidDownloadEndpoint", "/api/mobile/app/update/apk");

        AndroidAppUpdateService.UpdateCheckPayload payload =
                service.checkAndroidUpdate(null, "1.9.9", "https://fallback.example.com");

        assertTrue(payload.hasUpdate);
        assertEquals("https://fallback.example.com/api/mobile/app/update/apk?versionCode=200", payload.downloadUrl);
    }

    @Test
    void shouldRejectApkPathTraversal() throws Exception {
        Path manifestDir = Files.createDirectories(tempDir.resolve("manifest"));
        Path manifestPath = manifestDir.resolve("latest.json");

        String manifest = """
                {
                  "versionCode": 12,
                  "versionName": "1.2.0",
                  "apkFile": "../outside.apk"
                }
                """;
        Files.writeString(manifestPath, manifest, StandardCharsets.UTF_8);

        AndroidAppUpdateService service = new AndroidAppUpdateService();
        injectField(service, "androidManifestPath", manifestPath.toString());
        injectField(service, "androidDownloadBaseUrl", "");
        injectField(service, "androidDownloadEndpoint", "/api/mobile/app/update/apk");

        assertThrows(IllegalArgumentException.class, () -> service.resolveAndroidApk(null));
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
