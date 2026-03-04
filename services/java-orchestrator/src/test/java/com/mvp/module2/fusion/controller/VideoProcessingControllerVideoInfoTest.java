package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.service.FileReuseService;
import com.mvp.module2.fusion.service.Phase2bArticleLinkService;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VideoProcessingControllerVideoInfoTest {

    @Test
    void postVideoInfoShouldKeepBilibiliEpisodeQueryAndReturnEpisodePayload() throws Exception {
        VideoProcessingController controller = new VideoProcessingController();
        PythonGrpcClient.VideoInfoResult result = new PythonGrpcClient.VideoInfoResult();
        result.success = true;
        result.sourcePlatform = "bilibili";
        result.resolvedUrl = "https://www.bilibili.com/video/BV1n9CwYoEro";
        result.canonicalId = "BV1n9CwYoEro";
        result.videoTitle = "合集标题";
        result.durationSec = 202.0;
        result.isCollection = true;
        result.totalEpisodes = 2;
        result.currentEpisodeIndex = 2;
        result.currentEpisodeTitle = "第二集";
        result.contentType = "video";
        result.linkResolver = "canonical-no-redirect";
        result.coverUrl = "https://img.example.com/ep2.jpg";

        PythonGrpcClient.EpisodeInfo episode1 = new PythonGrpcClient.EpisodeInfo();
        episode1.index = 1;
        episode1.title = "第一集";
        episode1.durationSec = 101.0;
        episode1.episodeUrl = "https://www.bilibili.com/video/BV1n9CwYoEro?p=1";
        episode1.episodeCoverUrl = "https://img.example.com/ep1.jpg";
        PythonGrpcClient.EpisodeInfo episode2 = new PythonGrpcClient.EpisodeInfo();
        episode2.index = 2;
        episode2.title = "第二集";
        episode2.durationSec = 202.0;
        episode2.episodeUrl = "https://www.bilibili.com/video/BV1n9CwYoEro?p=2";
        episode2.episodeCoverUrl = "https://img.example.com/ep2.jpg";
        result.episodes = List.of(episode1, episode2);

        StubPythonGrpcClient stubGrpc = new StubPythonGrpcClient(result);
        injectField(controller, "pythonGrpcClient", stubGrpc);
        injectField(controller, "grpcTimeoutSeconds", 120);

        VideoProcessingController.VideoInfoRequest request = new VideoProcessingController.VideoInfoRequest();
        request.videoInput = "https://www.bilibili.com/video/BV1n9CwYoEro?spm_id_from=333.788.videopod.episodes&p=2";

        ResponseEntity<Map<String, Object>> response = controller.getVideoInfoByPost(request);

        assertEquals(200, response.getStatusCode().value());
        assertEquals(request.videoInput, stubGrpc.lastVideoInput);
        assertTrue(response.getBody() != null);
        Map<String, Object> payload = response.getBody();
        assertEquals(true, payload.get("success"));
        assertEquals(2, payload.get("totalEpisodes"));
        assertEquals(2, payload.get("currentEpisodeIndex"));
        assertEquals("第二集", payload.get("currentEpisodeTitle"));
        assertEquals("BV1n9CwYoEro_2", payload.get("rawEncodingKey"));
        assertEquals("https://img.example.com/ep2.jpg", payload.get("coverUrl"));
        Object episodes = payload.get("episodes");
        assertTrue(episodes instanceof List);
        assertEquals(2, ((List<?>) episodes).size());
        Object firstEpisode = ((List<?>) episodes).get(0);
        assertTrue(firstEpisode instanceof Map);
        assertEquals("https://img.example.com/ep1.jpg", ((Map<?, ?>) firstEpisode).get("episodeCoverUrl"));
    }

    @Test
    void postVideoInfoShouldRejectEmptyInput() {
        VideoProcessingController controller = new VideoProcessingController();
        VideoProcessingController.VideoInfoRequest request = new VideoProcessingController.VideoInfoRequest();
        request.videoInput = "   ";

        ResponseEntity<Map<String, Object>> response = controller.getVideoInfoByPost(request);

        assertEquals(400, response.getStatusCode().value());
    }

    @Test
    void postVideoInfoShouldPreferCachedProbePayloadWhenFingerprintPresent() throws Exception {
        VideoProcessingController controller = new VideoProcessingController();
        PythonGrpcClient.VideoInfoResult result = new PythonGrpcClient.VideoInfoResult();
        result.success = true;
        StubPythonGrpcClient stubGrpc = new StubPythonGrpcClient(result);

        FileReuseService reuseService = mock(FileReuseService.class);
        FileReuseService.FileFingerprint fingerprint =
                new FileReuseService.FileFingerprint("d41d8cd98f00b204e9800998ecf8427e", ".mp4");
        when(reuseService.normalizeFingerprint(any(), any(), any())).thenReturn(Optional.of(fingerprint));
        Map<String, Object> cached = new LinkedHashMap<>();
        cached.put("success", true);
        cached.put("title", "cached-probe-title");
        when(reuseService.findProbePayload(any())).thenReturn(Optional.of(cached));

        injectField(controller, "pythonGrpcClient", stubGrpc);
        injectField(controller, "fileReuseService", reuseService);
        injectField(controller, "grpcTimeoutSeconds", 120);

        VideoProcessingController.VideoInfoRequest request = new VideoProcessingController.VideoInfoRequest();
        request.videoInput = "D:\\videoToMarkdownTest2\\var\\uploads\\cached.mp4";
        request.fileMd5 = "d41d8cd98f00b204e9800998ecf8427e";
        request.fileExt = ".mp4";

        ResponseEntity<Map<String, Object>> response = controller.getVideoInfoByPost(request);

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody() != null);
        Map<String, Object> payload = response.getBody();
        assertEquals("cached-probe-title", payload.get("title"));
        assertEquals(true, payload.get("probeCacheHit"));
        assertEquals("d41d8cd98f00b204e9800998ecf8427e", payload.get("fileMd5"));
        assertEquals(".mp4", payload.get("fileExt"));
        assertEquals(null, stubGrpc.lastVideoInput);
    }

    @Test
    void postVideoInfoShouldProbeZhihuLinkWithoutGrpcCall() throws Exception {
        VideoProcessingController controller = new VideoProcessingController();
        StubPythonGrpcClient stubGrpc = new StubPythonGrpcClient(new PythonGrpcClient.VideoInfoResult());
        Phase2bArticleLinkService linkService = mock(Phase2bArticleLinkService.class);
        when(linkService.normalizeSupportedLinks(any())).thenReturn(List.of("https://zhuanlan.zhihu.com/p/123456"));
        when(linkService.prefetchLinkMetadata(any())).thenReturn(
                List.of(new Phase2bArticleLinkService.LinkMetadata(
                        "https://zhuanlan.zhihu.com/p/123456",
                        "zhihu",
                        "Zhihu Article Title",
                        "resolved"
                ))
        );

        injectField(controller, "pythonGrpcClient", stubGrpc);
        injectField(controller, "phase2bArticleLinkService", linkService);

        VideoProcessingController.VideoInfoRequest request = new VideoProcessingController.VideoInfoRequest();
        request.videoInput = "https://zhuanlan.zhihu.com/p/123456?utm_source=clipboard";

        ResponseEntity<Map<String, Object>> response = controller.getVideoInfoByPost(request);

        assertEquals(200, response.getStatusCode().value());
        assertNull(stubGrpc.lastVideoInput);
        assertTrue(response.getBody() != null);
        Map<String, Object> payload = response.getBody();
        assertEquals(true, payload.get("success"));
        assertEquals("book", payload.get("contentType"));
        assertEquals("https://zhuanlan.zhihu.com/p/123456", payload.get("resolvedUrl"));
        assertEquals("Zhihu Article Title", payload.get("title"));
    }

    @Test
    void postVideoInfoShouldTreatPdfPathWithTrailingShareTextAsBookInput() throws Exception {
        VideoProcessingController controller = new VideoProcessingController();
        StubPythonGrpcClient stubGrpc = new StubPythonGrpcClient(new PythonGrpcClient.VideoInfoResult());
        injectField(controller, "pythonGrpcClient", stubGrpc);

        VideoProcessingController.VideoInfoRequest request = new VideoProcessingController.VideoInfoRequest();
        request.videoInput = "D:\\videoToMarkdownTest2\\var\\uploads\\Distributed_Systems_4.pdf https://www.bilibili.com/video/BV1ABCDEF123";

        ResponseEntity<Map<String, Object>> response = controller.getVideoInfoByPost(request);

        assertEquals(503, response.getStatusCode().value());
        assertNull(stubGrpc.lastVideoInput);
        assertTrue(response.getBody() != null);
        assertEquals("book probe service unavailable", response.getBody().get("message"));
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class StubPythonGrpcClient extends PythonGrpcClient {
        private final VideoInfoResult fixedResult;
        private String lastVideoInput;

        private StubPythonGrpcClient(VideoInfoResult fixedResult) {
            this.fixedResult = fixedResult;
        }

        @Override
        public VideoInfoResult getVideoInfo(String taskId, String videoInput, int timeoutSec) {
            this.lastVideoInput = videoInput;
            return fixedResult;
        }
    }
}
