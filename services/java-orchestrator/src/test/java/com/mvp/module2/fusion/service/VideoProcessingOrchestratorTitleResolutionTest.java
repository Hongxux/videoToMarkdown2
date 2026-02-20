package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient.DownloadResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
