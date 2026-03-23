package com.mvp.module2.fusion.service;

import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VideoProcessingOrchestratorPhase2RecoveryTest {

    @Test
    void reconcileRecoveredRuntimeContextShouldRestoreVideoAndSubtitlePathsForStage1Resume() throws Exception {
        VideoProcessingOrchestrator orchestrator = new VideoProcessingOrchestrator();
        PythonGrpcClient grpcClient = mock(PythonGrpcClient.class);
        injectField(orchestrator, "grpcClient", grpcClient);

        Path taskRoot = Files.createTempDirectory("runtime-recovery-stage1");
        String videoPath = taskRoot.resolve("video.mp4").toString();
        String subtitlePath = taskRoot.resolve("subtitles.txt").toString();

        VideoProcessingOrchestrator.IOPhaseResult ioResult = new VideoProcessingOrchestrator.IOPhaseResult();
        ioResult.outputDir = taskRoot.toString();
        ioResult.videoUrl = "https://example.com/video";
        ioResult.videoPath = "https://example.com/video";
        ioResult.cleanupSourcePath = "https://example.com/video";
        ioResult.downloadResult = new PythonGrpcClient.DownloadResult();
        ioResult.downloadResult.videoPath = "https://example.com/video";

        PythonGrpcClient.RecoverRuntimeContextResult recoveryResult = new PythonGrpcClient.RecoverRuntimeContextResult();
        recoveryResult.success = true;
        recoveryResult.resolvedStartStage = "stage1";
        recoveryResult.downloadReady = true;
        recoveryResult.videoPath = videoPath;
        recoveryResult.videoDurationSec = 123.0d;
        recoveryResult.videoTitle = "Recovered Title";
        recoveryResult.resolvedUrl = "https://example.com/resolved";
        recoveryResult.sourcePlatform = "bilibili";
        recoveryResult.canonicalId = "BV1xx";
        recoveryResult.contentType = "video";
        recoveryResult.transcribeReady = true;
        recoveryResult.subtitlePath = subtitlePath;
        recoveryResult.stage1Ready = false;
        recoveryResult.phase2aReady = false;

        when(grpcClient.recoverRuntimeContext(
                "task-stage1",
                taskRoot.toString(),
                "stage1",
                "",
                "https://example.com/video",
                "",
                30
        )).thenReturn(recoveryResult);

        String resolvedStartStage = orchestrator.reconcileRecoveredRuntimeContext(
                "task-stage1",
                ioResult,
                "stage1",
                null
        );

        assertEquals("stage1", resolvedStartStage);
        assertEquals(videoPath, ioResult.videoPath);
        assertEquals(subtitlePath, ioResult.subtitlePath);
        assertEquals(123.0d, ioResult.videoDuration);
        assertEquals("Recovered Title", ioResult.metricsVideoTitle);
        assertNotNull(ioResult.downloadResult);
        assertTrue(ioResult.downloadResult.success);
        assertEquals("Recovered Title", ioResult.downloadResult.videoTitle);
        assertEquals("https://example.com/resolved", ioResult.downloadResult.resolvedUrl);
    }

    @Test
    void reconcileRecoveredPhase2ContextShouldPromoteStage1PlaceholderFromPythonRecovery() throws Exception {
        VideoProcessingOrchestrator orchestrator = new VideoProcessingOrchestrator();
        PythonGrpcClient grpcClient = mock(PythonGrpcClient.class);
        injectField(orchestrator, "grpcClient", grpcClient);

        Path taskRoot = Files.createTempDirectory("phase2-recovery-stage1");
        VideoProcessingOrchestrator.IOPhaseResult ioResult = new VideoProcessingOrchestrator.IOPhaseResult();
        ioResult.outputDir = taskRoot.toString();
        ioResult.videoUrl = "https://example.com/video";
        ioResult.videoPath = "https://example.com/video";
        ioResult.cleanupSourcePath = "https://example.com/video";
        ioResult.downloadResult = new PythonGrpcClient.DownloadResult();
        ioResult.downloadResult.videoPath = "https://example.com/video";

        PythonGrpcClient.RecoverRuntimeContextResult recoveryResult = new PythonGrpcClient.RecoverRuntimeContextResult();
        recoveryResult.success = true;
        recoveryResult.resolvedStartStage = "phase2a";
        recoveryResult.stage1Ready = true;
        recoveryResult.downloadReady = true;
        recoveryResult.videoPath = taskRoot.resolve("video.mp4").toString();
        recoveryResult.step2JsonPath = "";
        recoveryResult.step6JsonPath = "";
        recoveryResult.sentenceTimestampsPath = "";
        recoveryResult.phase2aReady = false;

        when(grpcClient.recoverRuntimeContext(
                "task-phase2a",
                taskRoot.toString(),
                "phase2a",
                "",
                "https://example.com/video",
                "",
                20
        )).thenReturn(recoveryResult);

        DynamicTimeoutCalculator.TimeoutConfig timeouts = new DynamicTimeoutCalculator.TimeoutConfig();
        timeouts.setPhase2aTimeoutSec(20);

        String resolvedStartStage = orchestrator.reconcileRecoveredPhase2Context(
                "task-phase2a",
                ioResult,
                "phase2a",
                timeouts
        );

        assertEquals("phase2a", resolvedStartStage);
        assertNotNull(ioResult.stage1Result);
        assertTrue(ioResult.stage1Result.success);
        assertEquals("", ioResult.stage1Result.step2JsonPath);
        assertEquals("", ioResult.stage1Result.step6JsonPath);
        assertEquals("", ioResult.stage1Result.sentenceTimestampsPath);
    }

    @Test
    void reconcileRecoveredPhase2ContextShouldAcceptPhase2aSemanticUnitsFromPythonRecovery() throws Exception {
        VideoProcessingOrchestrator orchestrator = new VideoProcessingOrchestrator();
        PythonGrpcClient grpcClient = mock(PythonGrpcClient.class);
        injectField(orchestrator, "grpcClient", grpcClient);

        Path taskRoot = Files.createTempDirectory("phase2-recovery-phase2a");
        String semanticUnitsPath = taskRoot.resolve("intermediates")
                .resolve("stages")
                .resolve("phase2a")
                .resolve("outputs")
                .resolve("semantic_units.json")
                .toString();

        VideoProcessingOrchestrator.IOPhaseResult ioResult = new VideoProcessingOrchestrator.IOPhaseResult();
        ioResult.outputDir = taskRoot.toString();
        ioResult.videoUrl = "https://example.com/video";
        ioResult.videoPath = "https://example.com/video";
        ioResult.cleanupSourcePath = "https://example.com/video";
        ioResult.downloadResult = new PythonGrpcClient.DownloadResult();
        ioResult.downloadResult.videoPath = "https://example.com/video";

        PythonGrpcClient.RecoverRuntimeContextResult recoveryResult = new PythonGrpcClient.RecoverRuntimeContextResult();
        recoveryResult.success = true;
        recoveryResult.resolvedStartStage = "asset_extract_java";
        recoveryResult.downloadReady = true;
        recoveryResult.videoPath = taskRoot.resolve("video.mp4").toString();
        recoveryResult.stage1Ready = true;
        recoveryResult.phase2aReady = true;
        recoveryResult.semanticUnitsPath = semanticUnitsPath;

        when(grpcClient.recoverRuntimeContext(
                "task-asset-extract",
                taskRoot.toString(),
                "phase2a",
                "",
                "https://example.com/video",
                "",
                18
        )).thenReturn(recoveryResult);

        DynamicTimeoutCalculator.TimeoutConfig timeouts = new DynamicTimeoutCalculator.TimeoutConfig();
        timeouts.setPhase2aTimeoutSec(18);

        String resolvedStartStage = orchestrator.reconcileRecoveredPhase2Context(
                "task-asset-extract",
                ioResult,
                "phase2a",
                timeouts
        );

        assertEquals("asset_extract_java", resolvedStartStage);
        assertEquals(semanticUnitsPath, ioResult.phase2aSemanticUnitsPath);
        assertNotNull(ioResult.stage1Result);
        assertTrue(ioResult.stage1Result.success);
    }

    @Test
    void reconcileRecoveredRuntimeContextShouldReturnCompletedWhenPhase2bOutputsAreReusable() throws Exception {
        VideoProcessingOrchestrator orchestrator = new VideoProcessingOrchestrator();
        PythonGrpcClient grpcClient = mock(PythonGrpcClient.class);
        injectField(orchestrator, "grpcClient", grpcClient);

        Path taskRoot = Files.createTempDirectory("phase2-recovery-completed");
        String markdownPath = taskRoot.resolve("result.md").toString();
        String jsonPath = taskRoot.resolve("result.json").toString();
        Files.writeString(Path.of(markdownPath), "# recovered");
        Files.writeString(Path.of(jsonPath), "{\"ok\":true}");

        VideoProcessingOrchestrator.IOPhaseResult ioResult = new VideoProcessingOrchestrator.IOPhaseResult();
        ioResult.outputDir = taskRoot.toString();
        ioResult.videoUrl = "https://example.com/video";
        ioResult.videoPath = taskRoot.resolve("video.mp4").toString();
        ioResult.cleanupSourcePath = ioResult.videoPath;

        PythonGrpcClient.RecoverRuntimeContextResult recoveryResult = new PythonGrpcClient.RecoverRuntimeContextResult();
        recoveryResult.success = true;
        recoveryResult.resolvedStartStage = "completed";
        recoveryResult.downloadReady = true;
        recoveryResult.videoPath = ioResult.videoPath;
        recoveryResult.phase2bReady = true;
        recoveryResult.markdownPath = markdownPath;
        recoveryResult.jsonPath = jsonPath;

        when(grpcClient.recoverRuntimeContext(
                "task-completed",
                taskRoot.toString(),
                "phase2b",
                "",
                ioResult.videoPath,
                "",
                30
        )).thenReturn(recoveryResult);

        String resolvedStartStage = orchestrator.reconcileRecoveredRuntimeContext(
                "task-completed",
                ioResult,
                "phase2b",
                null
        );
        VideoProcessingOrchestrator.ProcessingResult result =
                orchestrator.processVideoFromRecoveredOutputs("task-completed", ioResult);

        assertEquals("completed", resolvedStartStage);
        assertEquals(markdownPath, ioResult.phase2bMarkdownPath);
        assertEquals(jsonPath, ioResult.phase2bJsonPath);
        assertTrue(result.success);
        assertEquals(markdownPath, result.markdownPath);
        assertEquals(jsonPath, result.jsonPath);
    }

    private static void injectField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}