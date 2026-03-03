package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.service.AndroidAppUpdateAdminService;
import com.mvp.module2.fusion.service.AndroidAppUpdateService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockMultipartFile;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MobileAppUpdateControllerTest {

    @TempDir
    Path tempDir;

    @Test
    void checkAndroidUpdateShouldReturnStandardPayload() throws Exception {
        MobileAppUpdateController controller = new MobileAppUpdateController();
        StubAndroidAppUpdateService stubService = new StubAndroidAppUpdateService();
        stubService.payload = new AndroidAppUpdateService.UpdateCheckPayload(
                true,
                false,
                100,
                "1.0.0",
                120,
                "1.2.0",
                90,
                "2026-02-24T00:00:00Z",
                "bug fix",
                "https://api.example.com/api/mobile/app/update/apk?versionCode=120",
                "sha",
                321L
        );
        injectField(controller, "androidAppUpdateService", stubService);

        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setScheme("https");
        request.setServerName("api.example.com");
        request.setServerPort(443);

        ResponseEntity<Map<String, Object>> response = controller.checkAndroidUpdate(100, "1.0.0", request);

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody() != null);
        Map<String, Object> body = response.getBody();
        assertEquals(true, body.get("success"));
        assertEquals(true, body.get("hasUpdate"));
        assertEquals(120, body.get("latestVersionCode"));
        assertEquals("1.2.0", body.get("latestVersionName"));
    }

    @Test
    void downloadAndroidApkShouldReturnAttachment() throws Exception {
        MobileAppUpdateController controller = new MobileAppUpdateController();
        StubAndroidAppUpdateService stubService = new StubAndroidAppUpdateService();

        Path apkPath = tempDir.resolve("videoToMarkdown-1.2.0.apk");
        Files.write(apkPath, "apk-content".getBytes(StandardCharsets.UTF_8));
        stubService.apk = new AndroidAppUpdateService.ResolvedApk(
                apkPath,
                apkPath.getFileName().toString(),
                120,
                "1.2.0",
                "sha",
                Files.size(apkPath)
        );
        injectField(controller, "androidAppUpdateService", stubService);

        ResponseEntity<?> response = controller.downloadAndroidApk(120, null);

        assertEquals(200, response.getStatusCode().value());
        assertEquals("application/vnd.android.package-archive", response.getHeaders().getContentType().toString());
        assertEquals("bytes", response.getHeaders().getFirst(HttpHeaders.ACCEPT_RANGES));
        assertTrue(response.getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION).contains("attachment;"));
    }

    @Test
    void downloadAndroidApkShouldSupportRangeRequest() throws Exception {
        MobileAppUpdateController controller = new MobileAppUpdateController();
        StubAndroidAppUpdateService stubService = new StubAndroidAppUpdateService();

        Path apkPath = tempDir.resolve("videoToMarkdown-1.2.0.apk");
        Files.write(apkPath, "abcdef".getBytes(StandardCharsets.UTF_8));
        stubService.apk = new AndroidAppUpdateService.ResolvedApk(
                apkPath,
                apkPath.getFileName().toString(),
                120,
                "1.2.0",
                "sha",
                Files.size(apkPath)
        );
        injectField(controller, "androidAppUpdateService", stubService);

        ResponseEntity<?> response = controller.downloadAndroidApk(120, "bytes=1-3");

        assertEquals(206, response.getStatusCode().value());
        assertEquals("bytes", response.getHeaders().getFirst(HttpHeaders.ACCEPT_RANGES));
        assertEquals("bytes 1-3/6", response.getHeaders().getFirst(HttpHeaders.CONTENT_RANGE));
        assertEquals(3L, response.getHeaders().getContentLength());
    }

    @Test
    void uploadAndroidReleaseShouldRejectWhenTokenMissing() throws Exception {
        MobileAppUpdateController controller = new MobileAppUpdateController();
        injectField(controller, "adminToken", "secret-token");

        MockMultipartFile apkFile = new MockMultipartFile(
                "apk",
                "videoToMarkdown.apk",
                "application/vnd.android.package-archive",
                "apk".getBytes(StandardCharsets.UTF_8)
        );

        ResponseEntity<Map<String, Object>> response = controller.uploadAndroidRelease(
                null,
                null,
                apkFile,
                130,
                "1.3.0",
                120,
                false,
                "notes",
                true
        );
        assertEquals(401, response.getStatusCode().value());
        assertTrue(response.getBody() != null);
        assertEquals(false, response.getBody().get("success"));
    }

    @Test
    void publishAndroidReleaseShouldPassWithValidToken() throws Exception {
        MobileAppUpdateController controller = new MobileAppUpdateController();
        StubAndroidAppUpdateAdminService stubAdminService = new StubAndroidAppUpdateAdminService();
        stubAdminService.publishResult = new AndroidAppUpdateAdminService.PublishResult(
                130,
                "1.3.0",
                120,
                "D:/tmp/latest.json"
        );
        injectField(controller, "androidAppUpdateAdminService", stubAdminService);
        injectField(controller, "adminToken", "secret-token");

        ResponseEntity<Map<String, Object>> response = controller.publishAndroidRelease(
                "secret-token",
                null,
                130
        );
        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody() != null);
        assertEquals(true, response.getBody().get("success"));
        assertEquals(130, response.getBody().get("versionCode"));
        assertEquals(120, response.getBody().get("previousVersionCode"));
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class StubAndroidAppUpdateService extends AndroidAppUpdateService {
        private UpdateCheckPayload payload;
        private ResolvedApk apk;

        @Override
        public UpdateCheckPayload checkAndroidUpdate(
                Integer currentVersionCode,
                String currentVersionName,
                String requestBaseUrl
        ) {
            return payload;
        }

        @Override
        public ResolvedApk resolveAndroidApk(Integer requestedVersionCode) {
            return apk;
        }
    }

    private static final class StubAndroidAppUpdateAdminService extends AndroidAppUpdateAdminService {
        private PublishResult publishResult;

        @Override
        public PublishResult publishRelease(int versionCode) {
            return publishResult;
        }
    }
}
