package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.mock.web.MockMultipartFile;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AndroidAppUpdateAdminServiceTest {

    @TempDir
    Path tempDir;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void uploadReleaseShouldGenerateReleaseAndLatestManifest() throws Exception {
        Path latestManifestPath = tempDir.resolve("updates/latest.json");
        Path releasesDir = tempDir.resolve("updates/releases");
        Path historyPath = tempDir.resolve("updates/publish-history.json");

        AndroidAppUpdateAdminService service = new AndroidAppUpdateAdminService();
        injectField(service, "androidLatestManifestPath", latestManifestPath.toString());
        injectField(service, "androidReleasesDir", releasesDir.toString());
        injectField(service, "androidReleaseApkSubdir", "apk");
        injectField(service, "androidPublishHistoryPath", historyPath.toString());

        MockMultipartFile apkFile = new MockMultipartFile(
                "apk",
                "videoToMarkdown.apk",
                "application/vnd.android.package-archive",
                "apk-binary-v120".getBytes(StandardCharsets.UTF_8)
        );

        AndroidAppUpdateAdminService.UploadReleaseResult result = service.uploadRelease(
                apkFile,
                120,
                "1.2.0",
                110,
                false,
                "fix-1",
                true
        );

        assertTrue(result.published);
        assertEquals(120, result.versionCode);
        assertTrue(Files.exists(Path.of(result.releaseManifestPath)));
        assertTrue(Files.exists(latestManifestPath));
        assertTrue(Files.exists(historyPath));

        JsonNode latest = objectMapper.readTree(Files.newInputStream(latestManifestPath));
        assertEquals(120, latest.path("versionCode").asInt());
        assertEquals("1.2.0", latest.path("versionName").asText());
        assertEquals("fix-1", latest.path("releaseNotes").asText());
        assertTrue(latest.path("apkFile").asText().startsWith("releases/apk/"));
        assertNotNull(result.sha256);
        assertTrue(result.sha256.length() >= 32);
    }

    @Test
    void rollbackWithoutTargetShouldUsePreviousPublishedVersion() throws Exception {
        Path latestManifestPath = tempDir.resolve("updates/latest.json");
        Path releasesDir = tempDir.resolve("updates/releases");
        Path historyPath = tempDir.resolve("updates/publish-history.json");

        AndroidAppUpdateAdminService service = new AndroidAppUpdateAdminService();
        injectField(service, "androidLatestManifestPath", latestManifestPath.toString());
        injectField(service, "androidReleasesDir", releasesDir.toString());
        injectField(service, "androidReleaseApkSubdir", "apk");
        injectField(service, "androidPublishHistoryPath", historyPath.toString());

        MockMultipartFile apkV100 = new MockMultipartFile(
                "apk",
                "videoToMarkdown-v100.apk",
                "application/vnd.android.package-archive",
                "apk-v100".getBytes(StandardCharsets.UTF_8)
        );
        MockMultipartFile apkV200 = new MockMultipartFile(
                "apk",
                "videoToMarkdown-v200.apk",
                "application/vnd.android.package-archive",
                "apk-v200".getBytes(StandardCharsets.UTF_8)
        );

        service.uploadRelease(apkV100, 100, "1.0.0", 100, false, "v100", true);
        service.uploadRelease(apkV200, 200, "2.0.0", 150, true, "v200", true);

        AndroidAppUpdateAdminService.RollbackResult rollbackResult = service.rollbackRelease(null);
        assertEquals(200, rollbackResult.rolledBackFromVersionCode);
        assertEquals(100, rollbackResult.rolledBackToVersionCode);

        JsonNode latest = objectMapper.readTree(Files.newInputStream(latestManifestPath));
        assertEquals(100, latest.path("versionCode").asInt());
        assertEquals("1.0.0", latest.path("versionName").asText());
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
