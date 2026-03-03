package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient.DownloadResult;
import com.mvp.module2.fusion.service.Phase2bArticleLinkService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VideoProcessingOrchestratorTitleResolutionTest {

    @TempDir
    Path tempDir;

    @Test
    void resolveDocumentTitlePrefersGrpcTitleOverMetaAndFilename() throws Exception {
        VideoProcessingOrchestrator orchestrator = new VideoProcessingOrchestrator();
        DownloadResult downloadResult = new DownloadResult();
        downloadResult.videoTitle = "来自下载响应的标题";

        Path outputDir = tempDir.resolve("output");
        Files.createDirectories(outputDir);
        Path metaPath = outputDir.resolve("video_meta.json");
        Files.writeString(
            metaPath,
            new ObjectMapper().writeValueAsString(Map.of("title", "来自meta的标题"))
        );

        String actual = invokeResolveDocumentTitle(
            orchestrator,
            downloadResult,
            outputDir.toString(),
            outputDir.resolve("fallback-name.mp4").toString()
        );

        assertEquals("来自下载响应的标题", actual);
    }

    @Test
    void resolveDocumentTitleFallsBackToMetaWhenGrpcTitleEmpty() throws Exception {
        VideoProcessingOrchestrator orchestrator = new VideoProcessingOrchestrator();
        DownloadResult downloadResult = new DownloadResult();
        downloadResult.videoTitle = "   ";

        Path outputDir = tempDir.resolve("output-meta");
        Files.createDirectories(outputDir);
        Files.writeString(
            outputDir.resolve("video_meta.json"),
            new ObjectMapper().writeValueAsString(Map.of("title", "meta标题"))
        );

        String actual = invokeResolveDocumentTitle(
            orchestrator,
            downloadResult,
            outputDir.toString(),
            outputDir.resolve("fallback-name.mp4").toString()
        );

        assertEquals("meta标题", actual);
    }

    @Test
    void resolveDocumentTitleFallsBackToFilenameWhenNoTitleAvailable() throws Exception {
        VideoProcessingOrchestrator orchestrator = new VideoProcessingOrchestrator();
        DownloadResult downloadResult = new DownloadResult();
        downloadResult.videoTitle = "";

        Path outputDir = tempDir.resolve("output-fallback");
        Files.createDirectories(outputDir);
        Path videoPath = outputDir.resolve("final-name.mp4");

        String actual = invokeResolveDocumentTitle(
            orchestrator,
            downloadResult,
            outputDir.toString(),
            videoPath.toString()
        );

        assertEquals("final-name", actual);
    }

    @Test
    void buildDownloadTaskDirSourceShouldAppendEpisodeIndexForBilibiliPartUrl() throws Exception {
        VideoProcessingOrchestrator orchestrator = new VideoProcessingOrchestrator();
        String input = "https://www.bilibili.com/video/BV1n9CwYoEro?p=2";

        String actual = invokeBuildDownloadTaskDirSource(orchestrator, input);

        assertEquals("BV1n9CwYoEro_2", actual);
    }

    @Test
    void shouldProcessAsBookShouldReturnTrueForSupportedArticleLink() throws Exception {
        VideoProcessingOrchestrator orchestrator = new VideoProcessingOrchestrator();
        Phase2bArticleLinkService linkService = mock(Phase2bArticleLinkService.class);
        when(linkService.normalizeSupportedLinks(any())).thenReturn(List.of("https://juejin.cn/post/7390000000000000001"));
        injectField(orchestrator, "phase2bArticleLinkService", linkService);

        boolean actual = invokeShouldProcessAsBook(
                orchestrator,
                "https://juejin.cn/post/7390000000000000001",
                null
        );

        assertTrue(actual);
    }

    private String invokeResolveDocumentTitle(
        VideoProcessingOrchestrator orchestrator,
        DownloadResult downloadResult,
        String outputDir,
        String videoPath
    ) throws Exception {
        Method method = VideoProcessingOrchestrator.class.getDeclaredMethod(
            "resolveDocumentTitle",
            DownloadResult.class,
            String.class,
            String.class
        );
        method.setAccessible(true);
        return (String) method.invoke(orchestrator, downloadResult, outputDir, videoPath);
    }

    private String invokeBuildDownloadTaskDirSource(
        VideoProcessingOrchestrator orchestrator,
        String videoUrl
    ) throws Exception {
        Method method = VideoProcessingOrchestrator.class.getDeclaredMethod(
            "buildDownloadTaskDirSource",
            String.class
        );
        method.setAccessible(true);
        return (String) method.invoke(orchestrator, videoUrl);
    }

    private boolean invokeShouldProcessAsBook(
            VideoProcessingOrchestrator orchestrator,
            String source,
            VideoProcessingOrchestrator.BookProcessingOptions options
    ) throws Exception {
        Method method = VideoProcessingOrchestrator.class.getDeclaredMethod(
                "shouldProcessAsBook",
                String.class,
                VideoProcessingOrchestrator.BookProcessingOptions.class
        );
        method.setAccessible(true);
        return (boolean) method.invoke(orchestrator, source, options);
    }

    private void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
