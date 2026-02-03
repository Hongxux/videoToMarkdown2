package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.grpc.PythonGrpcClient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 视频处理编排器 (V3 Parallel)
 * 
 * 🔑 核心流程编排 (并行架构 V3)：
 * 1. 资源准备: 下载视频 (Download) 并执行 Whisper 语音转录 (Transcribe)，生成基础字幕与音频。
 * 2. 文本处理 (Stage1): 对字幕进行文本清洗、初级分割与结构化，生成候选分段。
 * 3. 语义分析 (Phase2A): 利用 LLM 对文本进行深层语义分析，划分精确的语义单元 (Semantic Units)，但不生成素材请求。
 * 4. 🚀 串行分析 (Serial Analysis):
 *    - 第一步: 视觉验证 (CV Validation)。通过 Python Workers 并行检查关键帧稳定性与动作类型，提取“动作单元”。
 *    - 第二步: 知识分类 (Knowledge Classification)。依赖 CV 提取的动作单元，调用 LLM 批量判断每个动作的知识类型 (原理/流程/事实)。
 * 5. 结果聚合 (Merge): 将 CV 视觉结果与基于动作的知识分类结果合并回语义单元，构建完整的上下文信息。
 * 6. 策略生成 (Material Policy): 基于合并后的信息，执行智能素材生成策略 (GenerateMaterialRequests)，决定每个单元最佳的展示形式 (截图 vs 视频片段)。
 * 7. 素材提取 (Excution): Java 端通过 FFmpeg JNI 高效并行提取所需的截图与切片，无需跨进程开销。
 * 8. 最终组装 (Phase2B): 将所有文本、视觉素材、布局信息发送至 Python 端，组装成最终的图文富文本 (Markdown/Docx)。
 */
@Service
public class VideoProcessingOrchestrator {
    
    private static final Logger logger = LoggerFactory.getLogger(VideoProcessingOrchestrator.class);
    
    @Autowired
    private PythonGrpcClient grpcClient;
    
    @Autowired
    private JavaCVFFmpegService ffmpegService;  // 🚀 使用 JNI 绑定，无进程开销
    
    @Autowired
    private DynamicTimeoutCalculator timeoutCalculator;
    
    @Autowired
    private CVValidationOrchestrator cvOrchestrator;
    
    @Autowired
    private KnowledgeClassificationOrchestrator knowledgeOrchestrator;
    
    // 任务管理
    private final ConcurrentHashMap<String, TaskContext> activeTasks = new ConcurrentHashMap<>();
    private final AtomicInteger taskCounter = new AtomicInteger(0);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // 进度回调 (Functional Interface)
    @FunctionalInterface
    public interface ProgressCallback {
        void onProgress(String taskId, double progress, String message);
    }
    private ProgressCallback progressCallback;
    public void setProgressCallback(ProgressCallback callback) { this.progressCallback = callback; }
    
    // --- Context Classes ---
    public static class ProcessingResult {
        public boolean success;
        public String taskId;
        public String markdownPath;
        public String jsonPath;
        public String errorMessage;
        public long processingTimeMs;
    }
    
    /**
     * 任务上下文
     */
    public static class TaskContext {
        public String taskId;
        public String videoUrl;
        public String outputDir;
        public double videoDuration;
        public long startTime;
    }

    /**
     * 同步处理视频 - 主入口
     */
    public ProcessingResult processVideo(String taskId, String videoUrl, String outputDir) {
        ProcessingResult result = new ProcessingResult();
        result.taskId = taskId;
        long startTime = System.currentTimeMillis();
        
        try {
            // 确保输出目录存在
            new File(outputDir).mkdirs();
            String videoPath = videoUrl;
            double videoDuration = 60; 

            // ========== Step 1: 下载视频 (Python) ==========
            if (videoUrl.startsWith("http")) {
                updateProgress(taskId, 0.05, "下载视频中...");
                DownloadResult dl = grpcClient.downloadVideoAsync(taskId, videoUrl, outputDir, 300).get(5, TimeUnit.MINUTES);
                if (!dl.success) throw new RuntimeException("Download failed: " + dl.errorMsg);
                videoPath = dl.videoPath;
                videoDuration = dl.durationSec;
                outputDir = new File(videoPath).getParentFile().getAbsolutePath(); 
            }
            
            DynamicTimeoutCalculator.TimeoutConfig timeouts = timeoutCalculator.calculateTimeouts(videoDuration);

            // 2. Transcribe
            updateProgress(taskId, 0.15, "语音转录中...");
            TranscribeResult tr = grpcClient.transcribeVideoAsync(taskId, videoPath, "auto", timeouts.getTranscribeTimeoutSec())
                .get(timeouts.getTranscribeTimeoutSec() + 60, TimeUnit.SECONDS);
            if (!tr.success) throw new RuntimeException("Transcribe failed: " + tr.errorMsg);
            
            // 3. Stage1
            updateProgress(taskId, 0.25, "Stage1 文本结构化...");
            Stage1Result s1 = grpcClient.processStage1Async(taskId, videoPath, tr.subtitlePath, outputDir, 6, timeouts.getStage1TimeoutSec())
                .get(timeouts.getStage1TimeoutSec() + 60, TimeUnit.SECONDS);
            if (!s1.success) throw new RuntimeException("Stage1 failed: " + s1.errorMsg);
            
            // 4. Phase2A (Segmentation)
            updateProgress(taskId, 0.35, "语义分割...");
            AnalyzeResult ar = grpcClient.analyzeSemanticUnitsAsync(taskId, videoPath, s1.step2JsonPath, s1.step6JsonPath, 
                s1.sentenceTimestampsPath, outputDir, timeouts.getPhase2aTimeoutSec())
                .get(timeouts.getPhase2aTimeoutSec() + 60, TimeUnit.SECONDS);
            if (!ar.success) throw new RuntimeException("Phase2A failed: " + ar.errorMsg);
            
            // 🔑 Load Semantic Units from JSON for Java processing
        File semanticFile = new File(ar.semanticUnitsJsonPath);
        JsonNode rootNode = objectMapper.readTree(semanticFile);
        final boolean originallyArray = rootNode.isArray();
        final Map<String, Object> unitsMap;
        final List<Map<String, Object>> unitsList;
        
        if (originallyArray) {
            unitsList = objectMapper.convertValue(rootNode, new TypeReference<List<Map<String, Object>>>() {});
            unitsMap = new HashMap<>(); // Dummy for the array branch
        } else {
            unitsMap = objectMapper.convertValue(rootNode, new TypeReference<Map<String, Object>>() {});
            List<Map<String, Object>> list = (List<Map<String, Object>>) unitsMap.get("semantic_units");
            unitsList = list != null ? list : new ArrayList<>();
        }
            
            // 5. 🚀 STAGED PARALLEL PIPELINE: 
            // - For Dynamic Units: CV -> Classify (Sequential)
            // - For Static Units: Classify (Immediate Parallel)
            updateProgress(taskId, 0.45, "执行级联并行分析 (CV -> 知识分类)...");
            
            Map<String, CVValidationUnitResult> cvResults = new ConcurrentHashMap<>();
            List<KnowledgeResultItem> classResults = Collections.synchronizedList(new ArrayList<>());
            List<CompletableFuture<?>> allFutures = new ArrayList<>();

            // A. Separate Units
            List<SemanticUnitInput> cvInputs = convertToCVInputs(unitsList);
            Set<String> cvUnitIds = cvInputs.stream().map(i -> i.unitId).collect(Collectors.toSet());
            
            List<Map<String, Object>> staticUnits = unitsList.stream()
                .filter(u -> !cvUnitIds.contains((String)u.get("unit_id")))
                .collect(Collectors.toList());

            // B. Branch 1: DYNAMIC Units (CV -> Classify)
            if (!cvInputs.isEmpty()) {
                List<CompletableFuture<CVBatchResult>> cvFutures = cvOrchestrator.validateBatchesAsync(taskId, videoPath, cvInputs, outputDir);
                if (cvFutures != null) {
                    for (CompletableFuture<CVBatchResult> cvFuture : cvFutures) {
                        // 🔗 Chain: When CV batch finishes, immediately start Classification for that specific batch
                        CompletableFuture<Void> chainedFuture = cvFuture.thenComposeAsync(batchRes -> {
                            if (batchRes.success && batchRes.results != null) {
                                // 1. Store CV results
                                for (CVValidationUnitResult r : batchRes.results) {
                                    cvResults.put(r.unitId, r);
                                }
                                
                                // 2. Convert to classification inputs (using CV results)
                                List<String> batchIds = batchRes.results.stream().map(r -> r.unitId).collect(Collectors.toList());
                                List<Map<String, Object>> batchUnits = unitsList.stream()
                                    .filter(u -> batchIds.contains((String)u.get("unit_id")))
                                    .collect(Collectors.toList());
                                
                                List<ClassificationInput> classInputs = convertToClassInputs(batchUnits, cvResults);
                                return knowledgeOrchestrator.classifyBatchAsync(taskId, classInputs)
                                    .thenAccept(classBatchRes -> {
                                        if (classBatchRes.success && classBatchRes.results != null) {
                                            classResults.addAll(classBatchRes.results);
                                        }
                                    });
                            }
                            return CompletableFuture.completedFuture(null);
                        });
                        allFutures.add(chainedFuture);
                    }
                }
            }

            // C. Branch 2: STATIC Units (Immediate Classify)
            if (!staticUnits.isEmpty()) {
                List<ClassificationInput> staticInputs = convertToClassInputs(staticUnits, cvResults);
                int batchSize = 5;
                for (int i = 0; i < staticInputs.size(); i += batchSize) {
                    List<ClassificationInput> batch = staticInputs.subList(i, Math.min(i + batchSize, staticInputs.size()));
                    CompletableFuture<ClassificationBatchResult> classFuture = knowledgeOrchestrator.classifyBatchAsync(taskId, batch);
                    allFutures.add(classFuture.thenAccept(batchRes -> {
                        if (batchRes.success && batchRes.results != null) {
                            classResults.addAll(batchRes.results);
                        }
                    }));
                }
            }

            // D. Wait for everything
            if (!allFutures.isEmpty()) {
                CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).join();
            }
            
            logger.info("✅ Staged Analysis done. CV: {}, Class: {}", cvResults.size(), classResults.size());
            
            // 6. Merge & Update
            updateSemanticUnits(unitsList, cvResults, classResults);
            saveUpdatedSemantics(semanticFile, originallyArray ? unitsList : unitsMap); // Save back for Python Phase2B
            
            // 7. Generate Material Requests
            // 6. Generate Material Requests (策略生成)
            updateProgress(taskId, 0.70, "生成素材清单...");
            List<MaterialGenerationInput> matInputs = convertToMatInputs(unitsList, cvResults);
            
            // 增加超时时间到 600s，避免高负载下超时
            logger.info("[{}] Step 6: Generating Material Requests...", taskId);
            MaterialGenerationResult matRes = grpcClient.generateMaterialRequestsAsync(taskId, matInputs, videoPath, 600).get(10, TimeUnit.MINUTES);
            
            if (!matRes.success) throw new RuntimeException("Material Gen failed: " + matRes.errorMsg);
            
            // 8. FFmpeg Extraction
            updateProgress(taskId, 0.80, "执行素材提取...");
            // Map proto requests to javaCV requests
            List<JavaCVFFmpegService.ScreenshotRequest> ssReqs = matRes.screenshotRequests.stream().map(r -> 
                new JavaCVFFmpegService.ScreenshotRequest(r.screenshotId, r.timestampSec, r.label, r.semanticUnitId))
                .collect(Collectors.toList());
            List<JavaCVFFmpegService.ClipRequest> clipReqs = matRes.clipRequests.stream().map(r ->
                new JavaCVFFmpegService.ClipRequest(r.clipId, r.startSec, r.endSec, r.knowledgeType, r.semanticUnitId))
                .collect(Collectors.toList());
                
            JavaCVFFmpegService.ExtractionResult extractRes = ffmpegService.extractAllSync(videoPath, outputDir, ssReqs, clipReqs, timeouts.getFfmpegTimeoutSec());
            
            // 9. Phase2B Assembly
            updateProgress(taskId, 0.90, "生成最终文档...");
            
            String title = new File(videoPath).getName().replace(".mp4", "");
            AssembleResult assembleRes = grpcClient.assembleRichTextAsync(taskId, videoPath, ar.semanticUnitsJsonPath, 
                outputDir + "/screenshots", outputDir + "/clips", outputDir, title, timeouts.getPhase2bTimeoutSec())
                .get(timeouts.getPhase2bTimeoutSec() + 60, TimeUnit.SECONDS);
                
            if (!assembleRes.success) throw new RuntimeException("Assemble failed: " + assembleRes.errorMsg);

            result.success = true;
            result.markdownPath = assembleRes.markdownPath;
            result.jsonPath = assembleRes.jsonPath;
            logger.info("✅ Pipeline Complete: {}", taskId);
            
        } catch (Exception e) {
            logger.error("❌ Pipeline Failed: {} - {}", taskId, e.getMessage());
            result.success = false;
            result.errorMessage = e.getMessage();
        } finally {
            result.processingTimeMs = System.currentTimeMillis() - startTime;
        }
        return result;
    }
    
    // --- Helpers ---
    private List<SemanticUnitInput> convertToCVInputs(List<Map<String, Object>> units) {
        return units.stream().map(u -> {
            SemanticUnitInput in = new SemanticUnitInput();
            in.unitId = (String) u.get("unit_id");
            in.title = (String) u.getOrDefault("full_text", u.getOrDefault("knowledge_topic", ""));
            in.text = (String) u.getOrDefault("full_text", u.getOrDefault("text", ""));
            in.startSec = parseDouble(u.getOrDefault("start_sec", u.get("timestamp_start")), 0.0);
            in.endSec = parseDouble(u.getOrDefault("end_sec", u.get("timestamp_end")), 0.0);
            in.knowledgeType = (String) u.getOrDefault("knowledge_type", "");
            return in;
        }).collect(Collectors.toList());
    }
    
    private List<ClassificationInput> convertToClassInputs(List<Map<String, Object>> units, Map<String, CVValidationUnitResult> cvResults) {
         return units.stream().map(u -> {
            ClassificationInput in = new ClassificationInput();
            String uid = (String) u.get("unit_id");
            in.unitId = uid;
            in.title = (String) u.get("title");
            in.text = (String) u.get("text");
            
            // 🚀 First, load action units from Stage 1/Phase 2A (if any)
            List<Map<String, Object>> aus = (List<Map<String, Object>>) u.get("action_units");
            if (aus != null) {
                for (Map<String, Object> au : aus) {
                    ActionSegmentResult as = new ActionSegmentResult();
                    as.startSec = parseDouble(au.get("start_sec"), 0.0);
                    as.endSec = parseDouble(au.get("end_sec"), 0.0);
                    in.actionUnits.add(as);
                }
            }
            
            // 🚀 SECOND, merge results from Parallel CV (which may have updated or added actions)
            if (cvResults.containsKey(uid)) {
                CVValidationUnitResult cvRes = cvResults.get(uid);
                if (cvRes.actionSegments != null) {
                    // If CV detected new segments, they take priority for classification
                    // For now, let's append them if actionUnits is empty, or merge carefully
                    if (in.actionUnits.isEmpty()) {
                        in.actionUnits.addAll(cvRes.actionSegments);
                    } else {
                        // TODO: Implement more complex merging if needed
                        // For now, we prefer the CV results as they are more accurate
                        in.actionUnits.clear();
                        in.actionUnits.addAll(cvRes.actionSegments);
                    }
                }
            }
            
            // Map Subtitles
            List<Map<String, Object>> subs = (List<Map<String, Object>>) u.get("subtitles");
            if (subs != null) {
                for (Map<String, Object> sub : subs) {
                    SubtitleItem si = new SubtitleItem();
                    si.startSec = parseDouble(sub.get("start_sec"), 0.0);
                    si.endSec = parseDouble(sub.get("end_sec"), 0.0);
                    si.text = (String) sub.get("text");
                    in.subtitles.add(si);
                }
            }
            return in;
        }).collect(Collectors.toList());
    }
    
    private List<MaterialGenerationInput> convertToMatInputs(List<Map<String, Object>> units, Map<String, CVValidationUnitResult> cvResults) {
        return units.stream().map(u -> {
            MaterialGenerationInput in = new MaterialGenerationInput();
            String uid = (String) u.get("unit_id");
            in.unitId = uid;
            in.startSec = parseDouble(u.getOrDefault("start_sec", u.get("timestamp_start")), 0.0);
            in.endSec = parseDouble(u.getOrDefault("end_sec", u.get("timestamp_end")), 0.0);
            in.knowledgeType = (String) u.getOrDefault("knowledge_type", "");
            in.fullText = (String) u.getOrDefault("full_text", u.getOrDefault("text", ""));
            
            // 🚀 Add CV Action Units
            if (cvResults.containsKey(uid)) {
                CVValidationUnitResult cvRes = cvResults.get(uid);
                if (cvRes.actionSegments != null) {
                    in.actionUnits.addAll(cvRes.actionSegments);
                }
            }
            return in;
        }).collect(Collectors.toList());
    }
    
    private void updateSemanticUnits(List<Map<String, Object>> units, 
                                     Map<String, CVValidationUnitResult> cvResults,
                                     List<KnowledgeResultItem> classResults) {
        // Map for fast lookup: unitId -> actionId -> result
        Map<String, Map<Integer, KnowledgeResultItem>> classMap = classResults.stream()
            .collect(Collectors.groupingBy(k -> k.unitId,
                Collectors.toMap(k -> k.actionId, k -> k, (k1, k2) -> k1)));
            
        for (Map<String, Object> unit : units) {
            String uid = (String) unit.get("unit_id");
            
            // 🚀 V7.6: Always update top-level knowledge_type first
            // This ensures Phase 2B Python pipeline sees the correct classification 
            // even if CV modality is screenshot (no actions) or other edge cases.
            if (classMap.containsKey(uid)) {
                Map<Integer, KnowledgeResultItem> unitRes = classMap.get(uid);
                String bestType = null;
                // Prefer Action-1, then Action-0, then any
                if (unitRes.containsKey(1)) bestType = unitRes.get(1).knowledgeType;
                else if (unitRes.containsKey(0)) bestType = unitRes.get(0).knowledgeType;
                else if (!unitRes.isEmpty()) bestType = unitRes.values().iterator().next().knowledgeType;
                
                if (bestType != null) {
                    unit.put("knowledge_type", bestType);
                }
            }
            
            // 1. Update CV results (Sync structure with Python expectation)
            if (cvResults.containsKey(uid)) {
                CVValidationUnitResult cvRes = cvResults.get(uid);
                unit.put("cv_validated", true);
                unit.put("modality", cvRes.modality);
                
                List<Map<String, Object>> actionsOut = new ArrayList<>();
                if (cvRes.actionSegments != null) {
                    for (ActionSegmentResult as : cvRes.actionSegments) {
                        Map<String, Object> actionMap = new java.util.HashMap<>();
                        actionMap.put("start_sec", as.startSec);
                        actionMap.put("end_sec", as.endSec);
                        actionMap.put("action_type", as.actionType);
                        actionMap.put("id", as.id);
                        
                        // 2. Apply Knowledge Type to this specific action
                        if (classMap.containsKey(uid) && classMap.get(uid).containsKey(as.id)) {
                            KnowledgeResultItem kri = classMap.get(uid).get(as.id);
                            actionMap.put("knowledge_type", kri.knowledgeType);
                            actionMap.put("reasoning", kri.reasoning);
                            actionMap.put("confidence", kri.confidence);
                        }
                        actionsOut.add(actionMap);
                    }
                }
                unit.put("action_units", actionsOut);
            }
        }
    }
    
    private double parseDouble(Object val, double defaultVal) {
        if (val == null) return defaultVal;
        if (val instanceof Number) return ((Number) val).doubleValue();
        try {
            return Double.parseDouble(val.toString());
        } catch (Exception e) {
            return defaultVal;
        }
    }

    private void saveUpdatedSemantics(File file, Object root) {
        try {
            objectMapper.writeValue(file, root);
        } catch(IOException e) {
            logger.error("Failed to save updated semantics", e);
        }
    }
    
    private void updateProgress(String taskId, double progress, String message) {
        if (progressCallback != null) progressCallback.onProgress(taskId, progress, message);
        logger.info("[{}] {} ({}%)", taskId, message, (int)(progress * 100));
    }
    
    public CompletableFuture<ProcessingResult> submitTaskAsync(String videoUrl, String outputDir) {
        String taskId = "task_" + taskCounter.incrementAndGet() + "_" + System.currentTimeMillis();
        return CompletableFuture.supplyAsync(() -> processVideo(taskId, videoUrl, outputDir));
    }
    
    public Map<String, TaskContext> getActiveTasks() { return new ConcurrentHashMap<>(activeTasks); }
}
