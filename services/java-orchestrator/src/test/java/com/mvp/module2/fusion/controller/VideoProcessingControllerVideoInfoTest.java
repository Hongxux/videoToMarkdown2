package com.mvp.module2.fusion.controller;

import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
