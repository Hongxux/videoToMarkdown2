package com.mvp.module2.fusion.service;

import com.mvp.module2.fusion.grpc.*;
import com.mvp.module2.fusion.grpc.client.PythonComputeClient;
import com.mvp.module2.fusion.model.EnhancementType;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class FusionDecisionService {

    private static final Logger logger = LoggerFactory.getLogger(FusionDecisionService.class);
    
    private final PythonComputeClient pythonClient;
    private final TextGeneratorService textGenerator;
    private final AdaptiveResourceOrchestrator resourceOrchestrator;
    private final com.fasterxml.jackson.databind.ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();

    public FusionDecisionService(PythonComputeClient pythonClient, 
                                 TextGeneratorService textGenerator,
                                 AdaptiveResourceOrchestrator resourceOrchestrator) {
        this.pythonClient = pythonClient;
        this.textGenerator = textGenerator;
        this.resourceOrchestrator = resourceOrchestrator;
    }

    /**
     * 🚀 FIXED: Unified decision flow - Always use makeDecisionStrict() for ALL cases
     * This aligns with Python's first principles approach where:
     * 1. Fault detection determines IF enhancement is needed
     * 2. makeDecisionStrict() determines WHICH material type to use
     */
    public CompletableFuture<String> processVideo(String videoPath, String transcription, double start, double end,
                                                  String sentencesPath, String subtitlesPath, String mergeDataPath, String mainTopic) {
        // Create output directory based on video path
        String outputDir = createOutputDirectory(videoPath);
        
        return CompletableFuture.supplyAsync(() -> {
            return pythonClient.extractFeatures(videoPath, start, end, transcription, sentencesPath, subtitlesPath, mergeDataPath, mainTopic); 
        }).thenApply(response -> {
            if (!response.getSuccess()) {
                logger.error("Feature Extraction Failed: {}", response.getErrorMessage());
                return "FAILED: " + response.getErrorMessage();
            }

            VisualFeatures visual = response.getVisualFeatures();
            SemanticFeatures semantic = response.getSemanticFeatures();
            
            // Step 2: Fault Detection (only to determine IF enhancement is needed)
            String checkText = visual.getOcrFullText() + " " + visual.getAsrSegmentText();
            DetectFaultsResponse faultResponse = pythonClient.detectFaults(visual, semantic, checkText, subtitlesPath, mergeDataPath, mainTopic);
            
            boolean hasFault = faultResponse.getHasFault();
            String faultType = hasFault ? faultResponse.getFaultType() : "NONE";
            
            if (hasFault) {
                logger.info("⚡ [Fault Detected] Type: {}, Reason: {}", faultType, faultResponse.getDetectionReason());
            }

            // 🔑 KEY FIX: ALWAYS use makeDecisionStrict() to determine material type
            // This is the Python first-principles approach: semantic + visual features determine material
            DecisionResult decision = makeDecisionStrictWithLag(semantic, visual, response.getDurationSec(), checkText);
            
            StringBuilder result = new StringBuilder();
            result.append("Type=").append(decision.type.getValue());
            result.append(", Reason='").append(decision.reason).append("'");
            result.append(", Lag=").append(String.format("%.2f", decision.lag)).append("s");
            
            if (hasFault) {
                // Generate supplementary text for faults
                GenerateTextResponse textResp = pythonClient.generateEnhancementText(visual, semantic, mainTopic, subtitlesPath, mergeDataPath, mainTopic);
                result.append(" | TextSupplement: ").append(textResp.getGeneratedText().substring(0, Math.min(100, textResp.getGeneratedText().length()))).append("...");
            }
            
            // 🔑 FIX: Apply visual lag compensation for screenshots
            double extendedEnd = end + decision.lag;
            
            // Step 5: Execute Material Generation based on Decision Type
            switch (decision.type) {
                case SCREENSHOT:
                    result.append(generateScreenshot(videoPath, start, extendedEnd, outputDir));
                    break;
                case VIDEO:
                    result.append(generateVideoClip(videoPath, start, end, outputDir, subtitlesPath, mergeDataPath, mainTopic));
                    break;
                case VIDEO_AND_SCREENSHOT:
                    // 🔑 FIX: Generate BOTH video and screenshot
                    String clipResult = generateVideoClip(videoPath, start, end, outputDir, subtitlesPath, mergeDataPath, mainTopic);
                    result.append(clipResult);
                    // Screenshot should be at extended end (after action completes)
                    result.append(generateScreenshot(videoPath, extendedEnd - 1.0, extendedEnd, outputDir));
                    break;
                case TEXT:
                default:
                    // Text-only, no media generation needed
                    result.append(" | MaterialType=TEXT_ONLY");
                    break;
            }
            
            return result.toString();
        });
    }
    
    /**
     * Create output directory for generated materials
     */
    private String createOutputDirectory(String videoPath) {
        java.io.File videoFile = new java.io.File(videoPath);
        String videoName = videoFile.getName().replaceAll("\\.[^.]+$", "");
        // Only use absolute path to ensure it goes to python_worker directory
        java.io.File outputDir = new java.io.File("D:\\videoToMarkdownTest2\\MVP_Module2_HEANCING\\enterprise_services\\python_worker\\output\\" + videoName);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }
        return outputDir.getAbsolutePath();
    }
    
    // =========================================================================
    // V7.x: Modality Classification 透传模式
    // =========================================================================
    
    /**
     * V7.x 新版素材补全流程 (透传模式)
     * 
     * Java只负责调度，决策逻辑完全由Python CVKnowledgeValidator执行。
     * 根据Python返回的modality直接生成对应素材。
     * 
     * @param videoPath 视频路径
     * @param startSec 开始时间
     * @param endSec 结束时间
     * @param subtitlesPath 字幕路径
     * @param mergeDataPath 合并数据路径
     * @param mainTopic 主题
     * @return CompletableFuture<MaterialResult>
     */
    public CompletableFuture<MaterialResult> processVideoWithModalityClassification(
            String videoPath, double startSec, double endSec,
            String subtitlesPath, String mergeDataPath, String mainTopic) {
        
        String outputDir = createOutputDirectory(videoPath);
        
        return CompletableFuture.supplyAsync(() -> {
            // Step 1: 调用Python获取V7.x模态分类结果
            var modalityResponse = pythonClient.getModalityClassification(videoPath, startSec, endSec);
            
            if (!modalityResponse.getSuccess()) {
                logger.error("V7.x ModalityClassification Failed: {}", modalityResponse.getErrorMessage());
                return new MaterialResult(false, "FAILED", modalityResponse.getErrorMessage());
            }
            
            String modality = modalityResponse.getModality();
            String subtype = modalityResponse.getKnowledgeSubtype();
            java.util.List<Double> screenshotTimes = modalityResponse.getScreenshotTimesList();
            
            logger.info("V7.x Modality Result: modality={}, subtype={}, screenshots={}", 
                       modality, subtype, screenshotTimes.size());
            
            // Step 2: 根据modality调度素材生成 (Java编排层核心职责)
            java.util.List<String> generatedPaths = new java.util.ArrayList<>();
            
            switch (modality) {
                case "screenshot":
                    // 截图: 使用Python返回的时间点，或使用末帧
                    double ssTime = screenshotTimes.isEmpty() ? endSec : screenshotTimes.get(0);
                    String ssResult = generateScreenshot(videoPath, ssTime - 0.1, ssTime + 0.1, outputDir);
                    generatedPaths.add(ssResult);
                    break;
                    
                case "video_only":
                    // 纯视频 (K4操作型)
                    String clipResult = generateVideoClip(videoPath, startSec, endSec, outputDir, 
                                                          subtitlesPath, mergeDataPath, mainTopic);
                    generatedPaths.add(clipResult);
                    break;
                    
                case "video_screenshot":
                    // 视频+截图 (K3推演型)
                    String videoResult = generateVideoClip(videoPath, startSec, endSec, outputDir,
                                                           subtitlesPath, mergeDataPath, mainTopic);
                    generatedPaths.add(videoResult);
                    
                    // 使用Python计算的最佳截图时间点
                    for (Double t : screenshotTimes) {
                        String screenshotResult = generateScreenshot(videoPath, t - 0.1, t + 0.1, outputDir);
                        generatedPaths.add(screenshotResult);
                    }
                    break;
                    
                case "discard":
                case "text_only":
                default:
                    // 无需生成素材
                    logger.info("V7.x: Skipping material generation for modality={}", modality);
                    break;
            }
            
            return new MaterialResult(true, modality, subtype, generatedPaths);
        });
    }
    
    /**
     * V7.x 素材生成结果
     */
    public static class MaterialResult {
        public final boolean success;
        public final String modality;
        public final String subtype;
        public final java.util.List<String> generatedPaths;
        public final String errorMessage;
        
        public MaterialResult(boolean success, String modality, String subtype, java.util.List<String> paths) {
            this.success = success;
            this.modality = modality;
            this.subtype = subtype;
            this.generatedPaths = paths;
            this.errorMessage = null;
        }
        
        public MaterialResult(boolean success, String modality, String errorMessage) {
            this.success = success;
            this.modality = modality;
            this.subtype = null;
            this.generatedPaths = java.util.Collections.emptyList();
            this.errorMessage = errorMessage;
        }
    }

    
    /**
     * Generate screenshot with error handling
     */
    private String generateScreenshot(String videoPath, double start, double end, String outputDir) {
        try {
            FrameSelectionResponse frame = pythonClient.selectBestFrame(videoPath, start, end);
            if (frame.getSuccess()) {
                return " | Screenshot: " + frame.getBestFramePath();
            } else {
                return " | Screenshot: FAILED";
            }
        } catch (Exception e) {
            logger.error("Screenshot generation failed: {}", e.getMessage());
            return " | Screenshot: ERROR-" + e.getMessage();
        }
    }
    
    /**
     * Generate video clip with error handling
     */
    private String generateVideoClip(String videoPath, double start, double end, String outputDir,
                                     String subtitlesPath, String mergeDataPath, String mainTopic) {
        try {
            VideoClipResponse clip = pythonClient.extractVideoClip(videoPath, start, end, outputDir, subtitlesPath, mergeDataPath, mainTopic);
            if (clip.getSuccess()) {
                return " | VideoClip: " + clip.getClipPath();
            } else {
                return " | VideoClip: FAILED";
            }
        } catch (Exception e) {
            logger.error("Video clip generation failed: {}", e.getMessage());
            return " | VideoClip: ERROR-" + e.getMessage();
        }
    }
    
    /**
     * Decision result structure matching Python's MultimodalDecision
     */
    private static class DecisionResult {
        EnhancementType type;
        String reason;
        double lag;
        
        DecisionResult(EnhancementType type, String reason, double lag) {
            this.type = type;
            this.reason = reason;
            this.lag = lag;
        }
    }
    
    /**
     * 🚀 NEW: Returns structured DecisionResult instead of string
     */
    private DecisionResult makeDecisionStrictWithLag(SemanticFeatures semantic, 
                                                      VisualFeatures visual,
                                                      double duration,
                                                      String faultText) {
        // Call existing logic but return structured result
        String resultStr = makeDecisionStrict(semantic, visual, duration, faultText);
        
        // Parse the result string to extract components
        EnhancementType type = EnhancementType.TEXT;
        String reason = "";
        double lag = 0.0;
        
        if (resultStr.contains("Type=VIDEO_AND_SCREENSHOT")) {
            type = EnhancementType.VIDEO_AND_SCREENSHOT;
        } else if (resultStr.contains("Type=VIDEO")) {
            type = EnhancementType.VIDEO;
        } else if (resultStr.contains("Type=SCREENSHOT")) {
            type = EnhancementType.SCREENSHOT;
        }
        
        // Extract reason
        int reasonStart = resultStr.indexOf("Reason='");
        int reasonEnd = resultStr.indexOf("'", reasonStart + 8);
        if (reasonStart >= 0 && reasonEnd > reasonStart) {
            reason = resultStr.substring(reasonStart + 8, reasonEnd);
        }
        
        // Extract lag
        int lagStart = resultStr.indexOf("Lag=");
        if (lagStart >= 0) {
            String lagStr = resultStr.substring(lagStart + 4).replaceAll("[^0-9.]", "");
            try {
                lag = Double.parseDouble(lagStr);
            } catch (NumberFormatException e) {
                lag = 0.5; // default
            }
        }
        
        return new DecisionResult(type, reason, lag);
    }

    // loadContext removed

    private String makeDecisionStrict(SemanticFeatures semantic, 
                                      VisualFeatures visual,
                                      double duration,
                                      String faultText) {
        
        // =========================================================================
        // 🟢 Step 0: Noise Filter (Pre-cleaning)
        // =========================================================================
        NoiseCheckResult noise = checkNoiseFilter(semantic, visual, duration, faultText);
        if (noise.isNoise) {
             // Discard by returning TEXT with low confidence (or a special NOISE type if supported)
             logger.info("🗑️ [Noise Filter] Discarded: {}", noise.reason);
            return formatResult(EnhancementType.TEXT, "Noise Filter: " + noise.reason, 0.0);
        }

        EnhancementType targetType = EnhancementType.TEXT;
        String anchorReason = "";
        
        // =========================================================================
        // 🔵 Step 1: First Principles Type Anchoring (Understanding Purpose)
        // =========================================================================
        String semanticType = semantic.getKnowledgeType(); // "process", "spatial", "abstract"
        boolean formula = visual.getHasMathFormula();

        // Specific Math Logic Sync (Phase 5/7)
        if (formula) {
            java.util.List<Float> ssimSeq = visual.getSsimSeqList();
            double minSsim = ssimSeq.isEmpty() ? 1.0 : java.util.Collections.min(ssimSeq);
            double avgMse = visual.getAvgMse();
            double edgeFlux = visual.getAvgEdgeFlux();
            
            String dynamicType = "UNKNOWN";
            if (minSsim < 0.8 && duration < 1.5) {
                 dynamicType = "TRANSITION";
            } else if (duration >= 2.0 && minSsim > 0.9 && edgeFlux > 0.05) {
                 dynamicType = "DERIVATION";
            } else if (avgMse < 50) {
                 dynamicType = "STATIC";
            }
            
            if ("TRANSITION".equals(dynamicType)) {
                 targetType = EnhancementType.SCREENSHOT;
                 anchorReason = "Math Transition Detected (Degraded to Static)";
            } else if ("DERIVATION".equals(dynamicType)) {
                 targetType = EnhancementType.VIDEO_AND_SCREENSHOT;
                 anchorReason = "Math Derivation Detected (Process Anchor)";
            } else {
                 targetType = EnhancementType.SCREENSHOT;
                 anchorReason = "Static Math Detected";
            }
            
            // Short-circuit for Math as in Python's refined logic
            double lag = calculateVisualLagStrict(targetType, visual, duration);
            return formatResult(targetType, "Concept Anchor Override: " + anchorReason, lag);
        }

        // 1. Understanding Evolution (Process)
        if ("process".equals(semanticType)) {
            // Cross-Check: Visual MUST be dynamic (or math formula which is always process)
            if (visual.getIsPotentialDynamic() || visual.getActionDensity() > 0.05 || visual.getHasMathFormula()) {
                targetType = EnhancementType.VIDEO;
                // Double Insurance: If result is complex structure, add Screenshot
                if (visual.getHasStaticVisualStructure() && visual.getElementCount() > 3) {
                     targetType = EnhancementType.VIDEO_AND_SCREENSHOT;
                     anchorReason = "Process Anchor (Evolution + Structure Result)";
                } else {
                     anchorReason = "Process Anchor (Dynamic Evolution)";
                }
            } else {
                // Degrade: Visual didn't support process.
                if (visual.getHasStaticVisualStructure()) {
                     targetType = EnhancementType.SCREENSHOT;
                     anchorReason = "Process Anchor Vetoed (Static Visual) -> Degraded to Spatial";
                } else {
                     targetType = EnhancementType.TEXT;
                     anchorReason = "Process Anchor Vetoed (No Visual Gain) -> Degraded to Text";
                }
            }
        
        // 2. Understanding Structure (Spatial)
        } else if ("spatial".equals(semanticType)) {
            // Cross-Check: Visual MUST have structure (or math formula)
            if (visual.getHasStaticVisualStructure() || visual.getElementCount() >= 2 || visual.getHasMathFormula()) {
                targetType = EnhancementType.SCREENSHOT;
                anchorReason = "Spatial Anchor (Structure/Logic)";
            } else {
                // Degrade: "Pseudo-Structure" (Teacher waving)
                targetType = EnhancementType.TEXT;
                anchorReason = "Spatial Anchor Vetoed (No Visual Structure) -> Degraded to Text";
            }
            
        // 3. Understanding Concept (Abstract)
        } else { // abstract
             if (visual.getHasStaticVisualStructure() && visual.getElementCount() > 5) {
                 // High density chart might be useful even for abstract talk
                 targetType = EnhancementType.SCREENSHOT;
                 anchorReason = "Concept Anchor Override (High Density Information)";
            } else {
                 targetType = EnhancementType.TEXT;
                 anchorReason = "Concept Anchor (Abstract/Definition)";
            }
        }

        // =========================================================================
        // 🟠 Step 2: Quality & Cognitive Threshold Validation
        // =========================================================================
        String valReason = "";
        EnhancementType finalType = targetType;
        
        // Calculate Visual Lag (Scenario Distinction)
        double lag = calculateVisualLagStrict(finalType, visual, duration);

        if (finalType == EnhancementType.VIDEO || finalType == EnhancementType.VIDEO_AND_SCREENSHOT) {
             // Validation: Clarity, Duration
             if (duration < 1.0) { 
                 finalType = EnhancementType.TEXT;
                 valReason = "Video too short (<1s)";
             } else if (visual.getAvgMse() > 500) { 
                 if (visual.getVisualConfidence() < 0.5) {
                      finalType = visual.getHasStaticVisualStructure() ? EnhancementType.SCREENSHOT : EnhancementType.TEXT;
                      valReason = "Low Video Confidence (Degraded)";
                 }
             }
        } else if (finalType == EnhancementType.SCREENSHOT) {
             // Validation: Visual Confidence
             if (visual.getVisualConfidence() < 0.6) {
                   finalType = EnhancementType.TEXT;
                   valReason = "Low Screenshot Quality (Blur/Incomplete)";
             }
        }

        String finalReason = anchorReason + (valReason.isEmpty() ? " -> Quality Pass" : " -> " + valReason);
        return formatResult(finalType, finalReason, lag);
    }
    
    private NoiseCheckResult checkNoiseFilter(SemanticFeatures semantic, 
                                              VisualFeatures visual, 
                                              double duration,
                                              String faultText) {
        // 1. Visual Noise: Dynamic action but no Gain
        boolean isDynamic = visual.getIsPotentialDynamic();
        
        if (isDynamic) {
             if ("abstract".equals(semantic.getKnowledgeType()) && !visual.getHasMathFormula() && visual.getElementCount() < 2) {
                 return new NoiseCheckResult(true, "Visual Noise: Dynamic Abstract Content (e.g. waving) without Structure");
             }
             // REMOVED: Redundant and conflicting check. 
             // Python's Decision Engine (judge_is_dynamic) already validated the action using complex rules (e.g. Peak Count).
             // If Python said "Dynamic" (even with low density), we should respect it.
             // if (visual.getActionDensity() < 0.1 && duration > 5.0 && !visual.getHasMathFormula()) {
             //    return new NoiseCheckResult(true, "Visual Noise: Sparse Action (<10%) in long clip");
             // }
        }

        // 2. Statistical Noise: Low visual info
        if (!isDynamic && !visual.getHasStaticVisualStructure() && visual.getElementCount() == 0) {
              if (!"abstract".equals(semantic.getKnowledgeType())) {
                   return new NoiseCheckResult(true, "Visual Noise: Empty Visuals for Non-Abstract content");
              }
        }

        // 3. Text Noise: (Sync with Python Rule 3)
        if (faultText == null || faultText.trim().length() < 2) {
             return new NoiseCheckResult(true, "Text Noise: Too short");
        }
        
        return new NoiseCheckResult(false, "");
    }
    
    private double calculateVisualLagStrict(EnhancementType type, VisualFeatures visual, double duration) {
        // Scenario 3: Dynamic Animation (Align to Animation End)
        if ((type == EnhancementType.VIDEO || type == EnhancementType.VIDEO_AND_SCREENSHOT) 
            && visual.getHasStaticVisualStructure()) {
             double animEnd = visual.getAnimationEndTime();
             if (animEnd > 0) {
                 // Lag = (AnimationEnd - VoiceEnd) + 0.2s Stable Time
                 double lag = animEnd - visual.getSegmentEnd() + 0.2;
                 return Math.max(0.2, lag);
             }
             return 3.0; // Fallback
        }
        // Scenario 2: Formula
        if (visual.getHasMathFormula()) return 2.5;
        // Scenario 2: Static Structure
        if (visual.getHasStaticVisualStructure()) return 1.5;
        // Scenario 1: Text
        return 0.5;
    }

    private String formatResult(EnhancementType type, String reason, double lag) {
        return String.format("DECISION: Type=%s, Reason='%s', Lag=%.2fs", type.getValue(), reason, lag);
    }

    public java.util.List<String> processFullVideo(String videoPath, String sentencesPath, String subtitlesPath, String mergeDataPath, String mainTopic) {
        try {
            // 🚀 FIX: Create proper output directory for results
            java.io.File videoFile = new java.io.File(videoPath);
            String videoName = videoFile.getName().replaceAll("\\.[^.]+$", "");
             // Only use absolute path to ensure it goes to python_worker directory
            java.io.File outputDir = new java.io.File("D:\\videoToMarkdownTest2\\MVP_Module2_HEANCING\\enterprise_services\\python_worker\\output\\" + videoName);
            if (!outputDir.exists()) {
                outputDir.mkdirs();
            }
            
            // 1. Load Data
            java.io.File mergeFile = new java.io.File(mergeDataPath);
            java.io.File timeFile = new java.io.File(sentencesPath);
            
            com.fasterxml.jackson.databind.JsonNode mergeRoot = objectMapper.readTree(mergeFile);
            com.fasterxml.jackson.databind.JsonNode timeRoot = objectMapper.readTree(timeFile);
            
            com.fasterxml.jackson.databind.JsonNode segments = mergeRoot.path("output").path("pure_text_script");
            
            logger.info("🚀 Starting BATCH Processing for {} segments...", segments.size());
            logger.info("📁 Output directory: {}", outputDir.getAbsolutePath());
            
            // 2. Prepare Batch Request
            java.util.List<FeatureRequest> featureRequests = new java.util.ArrayList<>();
            java.util.LinkedHashMap<String, com.fasterxml.jackson.databind.JsonNode> segmentMap = new java.util.LinkedHashMap<>();
            java.util.List<String> paragraphIds = new java.util.ArrayList<>(); // Preserve order

            // Context config
            ContextConfig contextConfig = ContextConfig.newBuilder()
                    .setSentencesPath(sentencesPath != null ? sentencesPath : "")
                    .setSubtitlesPath(subtitlesPath != null ? subtitlesPath : "")
                    .setMergeDataPath(mergeDataPath != null ? mergeDataPath : "")
                    .setMainTopic(mainTopic != null ? mainTopic : "")
                    .build();

            for (com.fasterxml.jackson.databind.JsonNode seg : segments) {
                String pId = seg.path("paragraph_id").asText();
                String text = seg.path("text").asText();
                com.fasterxml.jackson.databind.JsonNode sourceIds = seg.path("source_sentence_ids");
                if (sourceIds.size() == 0) continue;

                double start = 999999.0;
                double end = 0.0;
                for (com.fasterxml.jackson.databind.JsonNode sIdNode : sourceIds) {
                    String sId = sIdNode.asText();
                    com.fasterxml.jackson.databind.JsonNode timing = timeRoot.path(sId);
                    if (!timing.isMissingNode()) {
                        start = Math.min(start, timing.path("start_sec").asDouble());
                        end = Math.max(end, timing.path("end_sec").asDouble());
                    }
                }
                if (start > end) { start = 0; end = 10; }

                // Build Request
                FeatureRequest freq = FeatureRequest.newBuilder()
                        .setRequestId(pId) // Use paragraph ID as request ID for mapping
                        .setVideoPath(videoPath)
                        .setTimeRange(TimeRange.newBuilder().setStartSec(start).setEndSec(end).build())
                        .setConfig(AnalysisConfig.newBuilder().setEnableOcr(true).setMseThreshold(100.0).build())
                        .setSegmentText(text)
                        .setContextConfig(contextConfig)
                        .build();
                
                featureRequests.add(freq);
                segmentMap.put(pId, seg);
                paragraphIds.add(pId);
            }

            // 3. Execute Batch RPC
            logger.info("📡 Sending Batch Request with {} items...", featureRequests.size());
            BatchFeatureResponse batchResponse = pythonClient.extractFeaturesBatch(featureRequests);
            logger.info("✅ Batch Response Received. Processing Results...");

            // 4. Process Responses & Generate Materials
            java.util.List<java.util.Map<String, Object>> structuredResults = new java.util.ArrayList<>();
            java.util.List<String> stringResults = new java.util.ArrayList<>();

            // Create a map for fast lookup of responses
            java.util.Map<String, FeatureResponse> responseMap = new java.util.HashMap<>();
            if (batchResponse.getResponsesList() != null) {
                for (FeatureResponse r : batchResponse.getResponsesList()) {
                    responseMap.put(r.getRequestId(), r);
                }
            }

            // 5. Iterate in original order
            for (String pId : paragraphIds) {
                FeatureResponse resp = responseMap.get(pId);
                com.fasterxml.jackson.databind.JsonNode seg = segmentMap.get(pId);
                String text = seg.path("text").asText();
                
                // Recover start/end from request logic (simplification: re-calculate or store in map)
                // For brevity, re-calculating (it's fast)
                double start = 0; double end = 0; // Default
                com.fasterxml.jackson.databind.JsonNode sourceIds = seg.path("source_sentence_ids");
                 double minS = 999999.0; double maxE = 0.0;
                 for (com.fasterxml.jackson.databind.JsonNode sIdNode : sourceIds) {
                        String sId = sIdNode.asText();
                        com.fasterxml.jackson.databind.JsonNode timing = timeRoot.path(sId);
                        if (!timing.isMissingNode()) {
                            minS = Math.min(minS, timing.path("start_sec").asDouble());
                            maxE = Math.max(maxE, timing.path("end_sec").asDouble());
                        }
                 }
                 if (minS <= maxE) { start = minS; end = maxE; } else { start=0; end=10; } // Fallback

                java.util.Map<String, Object> resultMap = new java.util.HashMap<>();
                resultMap.put("paragraph_id", pId);
                resultMap.put("text", text);
                resultMap.put("start_sec", start);
                resultMap.put("end_sec", end);

                if (resp != null && resp.getSuccess()) {
                    // Success logic
                    try {
                        VisualFeatures visual = resp.getVisualFeatures();
                        SemanticFeatures semantic = resp.getSemanticFeatures();
                        double duration = end - start;
                        
                        // Decision Logic - 🚀 FIX: Pass segment text as faultText instead of empty string
                        DecisionResult decision = makeDecisionStrictWithLag(semantic, visual, duration, text);
                        
                        StringBuilder resultStr = new StringBuilder();
                        resultStr.append(formatResult(decision.type, decision.reason, decision.lag));
                        
                        // Material Generation (Sequential to avoid overloading)
                        double extendedEnd = end + decision.lag;
                        String outputDirPath = outputDir.getAbsolutePath();

                        if (decision.type == EnhancementType.SCREENSHOT) {
                           resultStr.append(generateScreenshot(videoPath, start, extendedEnd, outputDirPath));
                           resultMap.put("enhancement_type", "SCREENSHOT");
                        } else if (decision.type == EnhancementType.VIDEO) {
                           resultStr.append(generateVideoClip(videoPath, start, end, outputDirPath, subtitlesPath, mergeDataPath, mainTopic));
                           resultMap.put("enhancement_type", "VIDEO");
                        } else if (decision.type == EnhancementType.VIDEO_AND_SCREENSHOT) {
                           resultStr.append(generateVideoClip(videoPath, start, end, outputDirPath, subtitlesPath, mergeDataPath, mainTopic));
                           resultStr.append(generateScreenshot(videoPath, extendedEnd - 1.0, extendedEnd, outputDirPath)); // 1s buffer
                           resultMap.put("enhancement_type", "VIDEO_AND_SCREENSHOT");
                        } else {
                           resultMap.put("enhancement_type", "TEXT");
                        }
                        
                        resultMap.put("result", resultStr.toString());
                        resultMap.put("status", "SUCCESS");
                        
                        // Extract paths for JSON report
                        String s = resultStr.toString();
                        if (s.contains("Screenshot: ")) resultMap.put("screenshot_path", s.split("Screenshot: ")[1].trim());
                        if (s.contains("VideoClip: ")) resultMap.put("video_clip_path", s.split("VideoClip: ")[1].trim());

                        structuredResults.add(resultMap);
                        stringResults.add(pId + ": " + resultStr.toString());
                        
                    } catch (Exception e) {
                        resultMap.put("result", "Processing Error: " + e.getMessage());
                        resultMap.put("status", "FAILED");
                        structuredResults.add(resultMap);
                         stringResults.add(pId + ": FAILED - " + e.getMessage());
                    }
                } else {
                    // Fail logic
                    String err = (resp != null) ? resp.getErrorMessage() : "No response in batch";
                    resultMap.put("result", "FAILED - " + err);
                    resultMap.put("status", "FAILED");
                    structuredResults.add(resultMap);
                    stringResults.add(pId + ": FAILED - " + err);
                }
            }
            
            // 2. Export Final JSON Report (structured format matching Python)
            java.util.Map<String, Object> report = new java.util.LinkedHashMap<>();
            report.put("video_path", videoPath);
            report.put("output_dir", outputDir.getAbsolutePath());
            report.put("total_segments", structuredResults.size());
            report.put("enhancements", structuredResults);
            report.put("status", "COMPLETED");
            report.put("timestamp", java.time.Instant.now().toString());
            
            // Save to output directory instead of current directory
            java.io.File reportFile = new java.io.File(outputDir, "e2e_result.json");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(reportFile, report);
            logger.info("✅ Final Report generated: {}", reportFile.getAbsolutePath());
            
            // Also save legacy format for backward compatibility
            java.io.File legacyReportFile = new java.io.File("final_video_report.json");
            java.util.Map<String, Object> legacyReport = new java.util.HashMap<>();
            legacyReport.put("video_path", videoPath);
            legacyReport.put("segment_results", stringResults);
            legacyReport.put("status", "COMPLETED");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(legacyReportFile, legacyReport);
            
            return stringResults;
        } catch (Exception e) {
            logger.error("Batch Processing Failed: {}", e.getMessage(), e);
            return java.util.Collections.singletonList("FATAL ERROR: " + e.getMessage());
        }
    }

    private static class NoiseCheckResult {
        boolean isNoise;
        String reason;
        NoiseCheckResult(boolean isNoise, String reason) { this.isNoise = isNoise; this.reason = reason; }
    }
}
