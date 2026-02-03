package com.mvp.module2.fusion.grpc.client;

import com.mvp.module2.fusion.grpc.FeatureRequest;
import com.mvp.module2.fusion.grpc.FeatureResponse;
import com.mvp.module2.fusion.grpc.FusionComputeServiceGrpc;
import com.mvp.module2.fusion.grpc.VisualFeatures;
import com.mvp.module2.fusion.grpc.SemanticFeatures;
import com.mvp.module2.fusion.grpc.DetectFaultsRequest;
import com.mvp.module2.fusion.grpc.DetectFaultsResponse;
import com.mvp.module2.fusion.grpc.FrameSelectionRequest;
import com.mvp.module2.fusion.grpc.FrameSelectionResponse;
import com.mvp.module2.fusion.grpc.FrameHashRequest;
import com.mvp.module2.fusion.grpc.FrameHashResponse;
import com.mvp.module2.fusion.grpc.GenerateTextRequest;
import com.mvp.module2.fusion.grpc.GenerateTextResponse;
import com.mvp.module2.fusion.grpc.ContextConfig;
import com.mvp.module2.fusion.grpc.OptimizeMaterialsRequest;
import com.mvp.module2.fusion.grpc.OptimizeMaterialsResponse;
import com.mvp.module2.fusion.grpc.MaterialCandidate;
import com.mvp.module2.fusion.grpc.VideoClipRequest;
import com.mvp.module2.fusion.grpc.VideoClipResponse;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.grpc.StatusRuntimeException;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class PythonComputeClient {

    private static final Logger logger = LoggerFactory.getLogger(PythonComputeClient.class);

    @GrpcClient("python-compute-service")
    private FusionComputeServiceGrpc.FusionComputeServiceBlockingStub blockingStub;

    private void ensureStub() {
        if (blockingStub == null) {
            logger.error("gRPC blockingStub is NULL! @GrpcClient injection failed.");
            throw new IllegalStateException("gRPC client not initialized. Check application.properties and dependencies.");
        }
    }

    @CircuitBreaker(name = "pythonWorker", fallbackMethod = "fallbackExtractFeatures")
    public FeatureResponse extractFeatures(String videoPath, double start, double end, String segmentText, 
                                           String sentencesPath, String subtitlesPath, String mergeDataPath, String mainTopic) {
        logger.info("Sending RPC to Python Worker: {}", videoPath);
        
        ContextConfig contextConfig = ContextConfig.newBuilder()
                .setSentencesPath(sentencesPath != null ? sentencesPath : "")
                .setSubtitlesPath(subtitlesPath != null ? subtitlesPath : "")
                .setMergeDataPath(mergeDataPath != null ? mergeDataPath : "")
                .setMainTopic(mainTopic != null ? mainTopic : "")
                .build();

        FeatureRequest request = FeatureRequest.newBuilder()
                .setRequestId(java.util.UUID.randomUUID().toString())
                .setVideoPath(videoPath)
                .setTimeRange(com.mvp.module2.fusion.grpc.TimeRange.newBuilder()
                        .setStartSec(start)
                        .setEndSec(end)
                        .build())
                .setConfig(com.mvp.module2.fusion.grpc.AnalysisConfig.newBuilder()
                        .setEnableOcr(true)
                        .setMseThreshold(100.0)
                        .build())
                .setSegmentText(segmentText != null ? segmentText : "")
                .setContextConfig(contextConfig)
                .build();

        ensureStub();
        try {
            return blockingStub.withDeadlineAfter(1800, TimeUnit.SECONDS)
                               .extractFeatures(request);
        } catch (StatusRuntimeException e) {
            logger.error("RPC Failed: {}. Status: {}, Cause: {}", 
                         e.getMessage(), e.getStatus(), e.getCause(), e);
            throw e;
        }
    }

    // 🚀 NEW: Batch Processing RPC
    @CircuitBreaker(name = "pythonWorker", fallbackMethod = "fallbackExtractFeaturesBatch")
    public com.mvp.module2.fusion.grpc.BatchFeatureResponse extractFeaturesBatch(List<FeatureRequest> requests) {
         com.mvp.module2.fusion.grpc.BatchFeatureRequest request = com.mvp.module2.fusion.grpc.BatchFeatureRequest.newBuilder()
                 .addAllRequests(requests)
                 .build();
         ensureStub();
         try {
             // Long timeout for batch
             return blockingStub.withDeadlineAfter(3600, TimeUnit.SECONDS).extractFeaturesBatch(request);
         } catch (StatusRuntimeException e) {
             logger.error("RPC Batch Failed: {}", e.getStatus());
             throw e;
         }
    }

    // New RPC for Screenshot Selection
    @CircuitBreaker(name = "pythonWorker", fallbackMethod = "fallbackSelectBestFrame")
    public FrameSelectionResponse selectBestFrame(String videoPath, double start, double end) {
         FrameSelectionRequest request = FrameSelectionRequest.newBuilder()
                .setRequestId(java.util.UUID.randomUUID().toString())
                .setVideoPath(videoPath)
                .setStartSec(start)
                .setEndSec(end)
                .build();
         ensureStub();
         try {
             return blockingStub.withDeadlineAfter(1800, TimeUnit.SECONDS).selectBestFrame(request);
         } catch (StatusRuntimeException e) {
             logger.error("RPC SelectBestFrame Failed: {}", e.getStatus());
             throw e;
         }
    }

    // New RPC for Frame Hashing
    @CircuitBreaker(name = "pythonWorker", fallbackMethod = "fallbackComputeFrameHash")
    public FrameHashResponse computeFrameHash(String videoPath, List<Double> timestamps) {
        FrameHashRequest request = FrameHashRequest.newBuilder()
                .setRequestId(java.util.UUID.randomUUID().toString())
                .setVideoPath(videoPath)
                .addAllTimestamps(timestamps)
                .build();
        ensureStub();
        try {
            return blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS).computeFrameHash(request);
        } catch (StatusRuntimeException e) {
             logger.error("RPC ComputeFrameHash Failed: {}", e.getStatus());
             throw e;
        }
    }
    
    // New RPC for Strict Parity Fault Detection
    // New RPC for Strict Parity Fault Detection
    @CircuitBreaker(name = "pythonWorker", fallbackMethod = "fallbackDetectFaults")
    public DetectFaultsResponse detectFaults(VisualFeatures visual, 
                                             SemanticFeatures semantic, 
                                             String faultText,
                                             String subtitlesPath, String mergeDataPath, String mainTopic) {
        
        ContextConfig contextConfig = ContextConfig.newBuilder()
                .setSubtitlesPath(subtitlesPath != null ? subtitlesPath : "")
                .setMergeDataPath(mergeDataPath != null ? mergeDataPath : "")
                .setMainTopic(mainTopic != null ? mainTopic : "")
                .build();

        DetectFaultsRequest request = DetectFaultsRequest.newBuilder()
                .setRequestId(java.util.UUID.randomUUID().toString())
                .setVisualFeatures(visual)
                .setSemanticFeatures(semantic)
                .setFaultText(faultText)
                .setContextConfig(contextConfig)
                .build();
        ensureStub();
        try {
            return blockingStub.withDeadlineAfter(300, TimeUnit.SECONDS).detectFaults(request);
        } catch (StatusRuntimeException e) {
            logger.error("RPC DetectFaults Failed: {}", e.getStatus());
            throw e;
        }
    }

    // New RPC for Cognitive Text Generation
    // New RPC for Cognitive Text Generation
    @CircuitBreaker(name = "pythonWorker", fallbackMethod = "fallbackGenerateEnhancementText")
    public GenerateTextResponse generateEnhancementText(VisualFeatures visual, 
                                                        SemanticFeatures semantic, 
                                                        String domain,
                                                        String subtitlesPath, String mergeDataPath, String mainTopic) {

        ContextConfig contextConfig = ContextConfig.newBuilder()
                .setSubtitlesPath(subtitlesPath != null ? subtitlesPath : "")
                .setMergeDataPath(mergeDataPath != null ? mergeDataPath : "")
                .setMainTopic(mainTopic != null ? mainTopic : "")
                .build();

        GenerateTextRequest request = GenerateTextRequest.newBuilder()
                .setRequestId(java.util.UUID.randomUUID().toString())
                .setVisualFeatures(visual)
                .setSemanticFeatures(semantic)
                .setDomain(domain != null ? domain : "general")
                .setContextConfig(contextConfig)
                .build();
        ensureStub();
        try {
            return blockingStub.withDeadlineAfter(3000, TimeUnit.SECONDS).generateEnhancementText(request);
        } catch (StatusRuntimeException e) {
            logger.error("RPC GenerateEnhancementText Failed: {}", e.getStatus());
            throw e;
        }
    }

    @CircuitBreaker(name = "pythonWorker", fallbackMethod = "fallbackOptimizeMaterials")
    public OptimizeMaterialsResponse optimizeMaterials(String videoPath, List<MaterialCandidate> candidates) {
        OptimizeMaterialsRequest request = OptimizeMaterialsRequest.newBuilder()
                .setRequestId(java.util.UUID.randomUUID().toString())
                .setVideoPath(videoPath)
                .addAllCandidates(candidates)
                .build();
        ensureStub();
        try {
             // Long timeout for optimization (processing many images)
            return blockingStub.withDeadlineAfter(300, TimeUnit.SECONDS).optimizeMaterials(request);
        } catch (StatusRuntimeException e) {
            logger.error("RPC OptimizeMaterials Failed: {}", e.getStatus());
            throw e;
        }
    }

    // New RPC for Extract Video Clip
    @CircuitBreaker(name = "pythonWorker", fallbackMethod = "fallbackExtractVideoClip")
    public VideoClipResponse extractVideoClip(String videoPath, double start, double end, String outputDir, String subtitlesPath, String mergeDataPath, String mainTopic) {
        ContextConfig contextConfig = ContextConfig.newBuilder()
                .setSubtitlesPath(subtitlesPath != null ? subtitlesPath : "")
                .setMergeDataPath(mergeDataPath != null ? mergeDataPath : "")
                .setMainTopic(mainTopic != null ? mainTopic : "")
                .build();

        VideoClipRequest request = VideoClipRequest.newBuilder()
                .setRequestId(java.util.UUID.randomUUID().toString())
                .setVideoPath(videoPath)
                .setStartSec(start)
                .setEndSec(end)
                .setOutputDir(outputDir != null ? outputDir : "")
                .build();
        ensureStub();
        try {
            return blockingStub.withDeadlineAfter(300, TimeUnit.SECONDS).extractVideoClip(request);
        } catch (StatusRuntimeException e) {
            logger.error("RPC ExtractVideoClip Failed: {}", e.getStatus());
            throw e;
        }
    }

    // Fallbacks
    public FeatureResponse fallbackExtractFeatures(String videoPath, double start, double end, String segmentText, 
                                                   String sentencesPath, String subtitlesPath, String mergeDataPath, String mainTopic, Throwable t) {
        logger.warn("Circuit Breaker OPEN for ExtractFeatures. Error: {}", t.getMessage());
        return FeatureResponse.newBuilder().setSuccess(false).setErrorMessage("Fallback: " + t.getMessage()).build();
    }
    
    public FrameSelectionResponse fallbackSelectBestFrame(String videoPath, double start, double end, Throwable t) {
        logger.warn("Circuit Breaker OPEN for SelectBestFrame. Error: {}", t.getMessage());
         return FrameSelectionResponse.newBuilder().setSuccess(false).setErrorMessage("Fallback: " + t.getMessage()).build();
    }
    
    public FrameHashResponse fallbackComputeFrameHash(String videoPath, List<Double> timestamps, Throwable t) {
         logger.warn("Circuit Breaker OPEN for ComputeFrameHash. Error: {}", t.getMessage());
         return FrameHashResponse.newBuilder().setSuccess(false).setErrorMessage("Fallback: " + t.getMessage()).putAllTimestampToHash(Collections.emptyMap()).build();
    }

    public DetectFaultsResponse fallbackDetectFaults(VisualFeatures visual, 
                                                     SemanticFeatures semantic, 
                                                     String faultText,
                                                     String subtitlesPath, String mergeDataPath, String mainTopic, Throwable t) {
        logger.warn("Circuit Breaker OPEN for DetectFaults. Error: {}", t.getMessage());
        return DetectFaultsResponse.newBuilder().setHasFault(false).setDetectionReason("Fallback: " + t.getMessage()).build();
    }

    public GenerateTextResponse fallbackGenerateEnhancementText(VisualFeatures visual, 
                                                                SemanticFeatures semantic, 
                                                                String domain,
                                                                String subtitlesPath, String mergeDataPath, String mainTopic, Throwable t) {
         logger.warn("Circuit Breaker OPEN for GenerateEnhancementText. Error: {}", t.getMessage());
         return GenerateTextResponse.newBuilder().setSuccess(false).setGeneratedText("Note: Visual details require attention (Fallback).").build();
    }

    public OptimizeMaterialsResponse fallbackOptimizeMaterials(String videoPath, List<MaterialCandidate> candidates, Throwable t) {
         logger.warn("Circuit Breaker OPEN for OptimizeMaterials. Error: {}", t.getMessage());
         // Fallback: Keep everything (safe)
         return OptimizeMaterialsResponse.newBuilder().setSuccess(false).setErrorMessage("Fallback: " + t.getMessage()).build();
    }

    public VideoClipResponse fallbackExtractVideoClip(String videoPath, double start, double end, String outputDir, String subtitlesPath, String mergeDataPath, String mainTopic, Throwable t) {
        logger.warn("Circuit Breaker OPEN for ExtractVideoClip. Error: {}", t.getMessage());
        return VideoClipResponse.newBuilder().setSuccess(false).setErrorMessage("Fallback: " + t.getMessage()).build();
    }

    public com.mvp.module2.fusion.grpc.BatchFeatureResponse fallbackExtractFeaturesBatch(List<FeatureRequest> requests, Throwable t) {
        logger.warn("Circuit Breaker OPEN for ExtractFeaturesBatch. Error: {}", t.getMessage());
        return com.mvp.module2.fusion.grpc.BatchFeatureResponse.newBuilder().build(); // Empty response
    }

    // =========================================================================
    // V7.x: Modality Classification RPC
    // =========================================================================
    
    @CircuitBreaker(name = "pythonWorker", fallbackMethod = "fallbackGetModalityClassification")
    public com.mvp.module2.fusion.grpc.ModalityClassificationResponse getModalityClassification(
            String videoPath, double startSec, double endSec) {
        logger.info("V7.x ModalityClassification RPC: {} [{:.1f}s-{:.1f}s]", videoPath, startSec, endSec);
        
        com.mvp.module2.fusion.grpc.ModalityClassificationRequest request = 
            com.mvp.module2.fusion.grpc.ModalityClassificationRequest.newBuilder()
                .setRequestId(java.util.UUID.randomUUID().toString())
                .setVideoPath(videoPath)
                .setStartSec(startSec)
                .setEndSec(endSec)
                .build();
        
        ensureStub();
        try {
            return blockingStub.withDeadlineAfter(300, TimeUnit.SECONDS)
                               .getModalityClassification(request);
        } catch (StatusRuntimeException e) {
            logger.error("ModalityClassification RPC Failed: {}", e.getMessage(), e);
            throw e;
        }
    }
    
    public com.mvp.module2.fusion.grpc.ModalityClassificationResponse fallbackGetModalityClassification(
            String videoPath, double startSec, double endSec, Throwable t) {
        logger.warn("Circuit Breaker OPEN for ModalityClassification. Error: {}", t.getMessage());
        // Fallback: Return screenshot modality (safe default)
        return com.mvp.module2.fusion.grpc.ModalityClassificationResponse.newBuilder()
                .setSuccess(false)
                .setErrorMessage("Fallback: " + t.getMessage())
                .setModality("screenshot")
                .build();
    }
}

