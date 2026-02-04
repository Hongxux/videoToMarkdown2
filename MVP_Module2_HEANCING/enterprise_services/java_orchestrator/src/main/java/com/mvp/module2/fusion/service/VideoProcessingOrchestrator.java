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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.Locale;

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
            String videoPath = videoUrl;
            double videoDuration = 60; 
            
            // 统一本地任务输出目录：做什么是将本地路径映射到 storage/{hash}；为什么是保证中间产物集中；权衡是会新增一次文件复制/硬链接成本
            if (!isHttpUrl(videoUrl)) {
                videoPath = normalizeLocalVideoPath(videoUrl);
                outputDir = resolveOutputDirForLocalVideo(videoPath);
                new File(outputDir).mkdirs();
                logger.info("[{}] 统一本地任务输出目录 -> {}", taskId, outputDir);
                
                // 将本地视频复制/硬链接到 storage/{hash}：做什么是让视频与产物同域；为什么是便于回放与清理；权衡是增加一次磁盘写入或链接操作
                videoPath = ensureLocalVideoInStorage(videoPath, outputDir);
            }

            // ========== Step 1: 下载视频 (Python) ==========（做什么：拉取视频；为什么：统一产物目录；权衡：依赖网络与 I/O）
            if (isHttpUrl(videoUrl)) {
                updateProgress(taskId, 0.05, "下载视频中..");
                DownloadResult dl = grpcClient.downloadVideoAsync(taskId, videoUrl, outputDir, 300).get(5, TimeUnit.MINUTES);
                if (!dl.success) throw new RuntimeException("Download failed: " + dl.errorMsg);
                videoPath = dl.videoPath;
                videoDuration = dl.durationSec;
                outputDir = new File(videoPath).getParentFile().getAbsolutePath(); 
                new File(outputDir).mkdirs();
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
            
            // ❌ Removed: enrichUnitsWithSubtitles - Classifier now reads directly from Step 2
            
            // 5. 🚀 UNIFIED PARALLEL PIPELINE (Optimized for LPT)
            // Send ALL units to Python. Python will handle CV vs CF routing and LPT scheduling.
            updateProgress(taskId, 0.45, "执行级联并行分析 (CV/CF 混合调度)...");
            
            Map<String, CVValidationUnitResult> cvResults = new ConcurrentHashMap<>();
            List<KnowledgeResultItem> classResults = Collections.synchronizedList(new ArrayList<>());
            List<CompletableFuture<?>> allFutures = Collections.synchronizedList(new ArrayList<>());
            List<CompletableFuture<Boolean>> cvFuturesList = new ArrayList<>();

                        // A. Convert ALL units to CV Inputs & Sort (Weighted LPT)
            List<SemanticUnitInput> allInputs = convertToCVInputs(unitsList);

            // 尝试复用 CV + LLM 缓存：做什么是命中就跳过计算；为什么是降低成本；权衡是缓存可能过期
            Map<String, CVValidationUnitResult> cachedCv = cvOrchestrator.tryLoadCachedResults(taskId, videoPath, allInputs, outputDir);
            boolean cvCacheHit = cachedCv != null && !cachedCv.isEmpty();
            boolean classCacheHit = false;
            List<ClassificationInput> cachedClassInputs = null;

            if (cvCacheHit) {
                cvResults.putAll(cachedCv);
                cachedClassInputs = convertToClassInputs(unitsList, cvResults);
                List<KnowledgeResultItem> cachedClass = knowledgeOrchestrator.tryLoadCachedResults(
                    taskId, cachedClassInputs, s1.step2JsonPath, outputDir);
                if (cachedClass != null && !cachedClass.isEmpty()) {
                    classResults.addAll(cachedClass);
                    classCacheHit = true;
                }
            }

            if (!cvCacheHit) {
                // Weighted LPT Scheduling (Java Side)
                // Weight: CV(Process/Practical)=10, CF(Abstract/Explanation)=1
                // Score = Duration * Weight
                // Sort: Descending by Score
                Collections.sort(allInputs, (o1, o2) -> {
                    double w1 = isCVTask(o1.knowledgeType) ? 10.0 : 1.0;
                    double w2 = isCVTask(o2.knowledgeType) ? 10.0 : 1.0;
                    double score1 = (o1.endSec - o1.startSec) * w1;
                    double score2 = (o2.endSec - o2.startSec) * w2;
                    return Double.compare(score2, score1); // Descending
                });

                // B. Batch Validate (CV & CF mixed) with Streaming Callbacks
                if (!allInputs.isEmpty()) {
                    List<CompletableFuture<Boolean>> cvFutures = cvOrchestrator.validateBatchesAsync(taskId, videoPath, allInputs, outputDir, unitResult -> {
                        // Callback: Triggered immediately when Python finishes a single unit
                        cvResults.put(unitResult.unitId, unitResult);

                        // Immediate Chain: Individual unit classification
                        List<Map<String, Object>> unitToClassify = unitsList.stream()
                            .filter(u -> unitResult.unitId.equals((String)u.get("unit_id")))
                            .collect(Collectors.toList());

                        if (!unitToClassify.isEmpty()) {
                            List<ClassificationInput> classInputs = convertToClassInputs(unitToClassify, cvResults);

                            CompletableFuture<Void> classFuture = knowledgeOrchestrator.classifyBatchAsync(taskId, classInputs, s1.step2JsonPath)
                                .thenAccept(classBatchRes -> {
                                    if (classBatchRes.success && classBatchRes.results != null) {
                                        classResults.addAll(classBatchRes.results);
                                        logger.info("[{}] Incremental classification done for: {}", taskId, unitResult.unitId);
                                    }
                                });

                            // Add to global watch list (using a dedicated collection to avoid CME)
                            allFutures.add(classFuture);
                        }
                    });

                    if (cvFutures != null) {
                        for (CompletableFuture<Boolean> cvFuture : cvFutures) {
                            cvFuturesList.add(cvFuture);
                        }
                    }
                }

                // D. Wait for CV/CF streaming to complete (this triggers all callbacks)
                if (!cvFuturesList.isEmpty()) {
                    CompletableFuture.allOf(cvFuturesList.toArray(new CompletableFuture[0])).join();
                }

                // E. Wait for all triggered Classification tasks to finish
                while (true) {
                    CompletableFuture<?>[] pending;
                    synchronized(allFutures) {
                        pending = allFutures.stream().filter(f -> !f.isDone()).toArray(CompletableFuture[]::new);
                    }
                    if (pending.length == 0) break;
                    CompletableFuture.allOf(pending).join();
                }
            } else if (!classCacheHit) {
                // CV 缓存命中但 LLM 未命中：做什么是补齐分类；为什么是保证结果完整；权衡是增加一次 LLM 调用
                List<ClassificationInput> classInputs = cachedClassInputs != null ? cachedClassInputs : convertToClassInputs(unitsList, cvResults);
                List<KnowledgeResultItem> batchRes = knowledgeOrchestrator.classifyParallel(taskId, classInputs, s1.step2JsonPath, outputDir);
                if (batchRes != null) {
                    classResults.addAll(batchRes);
                }
            } else {
                logger.info("[{}] Reusing CV + LLM caches, skip Phase2 analysis", taskId);
            }

            // 缓存落盘：做什么是保存计算结果；为什么是便于重跑；权衡是占用磁盘
            if (!cvResults.isEmpty()) {
                cvOrchestrator.saveCache(taskId, videoPath, allInputs, outputDir, cvResults);
            }
            if (!classResults.isEmpty()) {
                List<ClassificationInput> classInputsForCache = cachedClassInputs != null ? cachedClassInputs : convertToClassInputs(unitsList, cvResults);
                knowledgeOrchestrator.saveCache(taskId, classResults, classInputsForCache, s1.step2JsonPath, outputDir);
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
            // 合并 Phase2A 初始素材请求与后续生成请求：做什么是避免数据被忽略；为什么是保留上游召回；权衡是可能增加素材数量
            List<JavaCVFFmpegService.ScreenshotRequest> ssReqs = mergeScreenshotRequests(ar.screenshotRequests, matRes.screenshotRequests);
            List<JavaCVFFmpegService.ClipRequest> clipReqs = mergeClipRequests(ar.clipRequests, matRes.clipRequests);
            
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

    // --- OutputDir 统一规则 ---
    private boolean isHttpUrl(String value) {
        if (value == null) return false;
        String lower = value.toLowerCase(Locale.ROOT);
        return lower.startsWith("http://") || lower.startsWith("https://");
    }

    private String normalizeLocalVideoPath(String videoUrl) {
        // 统一处理 file:// 和相对路径，避免 hash 因路径格式不同而漂移
        try {
            if (videoUrl != null && videoUrl.toLowerCase(Locale.ROOT).startsWith("file://")) {
                return Paths.get(URI.create(videoUrl)).toAbsolutePath().normalize().toString();
            }
        } catch (Exception e) {
            logger.warn("Failed to parse file URI, fallback to raw path: {}", videoUrl);
        }
        return new File(videoUrl).getAbsolutePath();
    }

    private String resolveOutputDirForLocalVideo(String videoPath) {
        Path storageRoot = resolveStorageRoot();
        try {
            Path absVideoPath = Paths.get(videoPath).toAbsolutePath().normalize();
            if (isUnderPath(absVideoPath, storageRoot)) {
                return absVideoPath.getParent().toString();
            }
        } catch (Exception e) {
            // 解析失败时走 hash 路径，避免阻断主流程
        }

        String normalized = normalizePathForHash(videoPath);
        String hash = md5Hex(normalized);
        return storageRoot.resolve(hash).toString();
    }

    private String ensureLocalVideoInStorage(String videoPath, String outputDir) {
        // 将本地视频复制/硬链接到 storage/{hash}：做什么是让视频与产物同域；为什么是便于回放与清理；权衡是增加一次 I/O
        try {
            Path source = Paths.get(videoPath).toAbsolutePath().normalize();
            Path storageRoot = resolveStorageRoot();
            if (isUnderPath(source, storageRoot)) {
                return source.toString();
            }

            Path targetDir = Paths.get(outputDir).toAbsolutePath().normalize();
            Files.createDirectories(targetDir);
            String fileName = source.getFileName().toString();
            Path target = targetDir.resolve(fileName);

            if (Files.exists(target)) {
                return target.toString();
            }

            try {
                Files.createLink(target, source);
                logger.info("Linked local video into storage: {}", target);
                return target.toString();
            } catch (Exception linkError) {
                // 硬链接失败就复制：做什么是降级保证；为什么是跨盘/权限限制常见；权衡是多一次磁盘写入
            }

            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
            logger.info("Copied local video into storage: {}", target);
            return target.toString();
        } catch (Exception e) {
            logger.warn("Failed to place local video in storage, fallback to original path: {}", videoPath);
            return videoPath;
        }
    }

    private Path resolveStorageRoot() {
        // 通过仓库根目录定位 storage：做什么是让 Java/Python 产物同域；为什么是便于整理与回放；权衡是依赖工作目录结构
        Path repoRoot = resolveRepoRoot();
        Path storageRoot = repoRoot.resolve("storage").toAbsolutePath().normalize();
        try {
            Files.createDirectories(storageRoot);
        } catch (IOException e) {
            logger.warn("Failed to create storage root: {}", storageRoot);
        }
        return storageRoot;
    }

    private Path resolveRepoRoot() {
        // 逐层向上寻找仓库根标记，找不到则退回当前工作目录

        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
        for (int i = 0; i < 6; i++) {
            if (Files.exists(current.resolve("python_grpc_server.py"))
                || (Files.isDirectory(current.resolve("proto")) && Files.isDirectory(current.resolve("MVP_Module2_HEANCING")))) {
                return current;
            }
            Path parent = current.getParent();
            if (parent == null) break;
            current = parent;
        }
        return Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();
    }

    private boolean isUnderPath(Path path, Path root) {
        try {
            return path.toAbsolutePath().normalize().startsWith(root.toAbsolutePath().normalize());
        } catch (Exception e) {
            return false;
        }
    }

    private String normalizePathForHash(String path) {
        // 统一 hash 输入：做什么是归一化路径；为什么是保证 Java/Python 结果一致；权衡是忽略符号链接真实路径
        String abs = new File(path).getAbsolutePath();
        String normalized = abs.replace('/', File.separatorChar);
        if (File.separatorChar == '\\') {
            normalized = normalized.toLowerCase(Locale.ROOT);
        }
        return normalized;
    }

    private String md5Hex(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("MD5 hash failed", e);
        }
    }

    // --- 素材请求合并 ---
    private List<JavaCVFFmpegService.ScreenshotRequest> mergeScreenshotRequests(
            List<PythonGrpcClient.ScreenshotRequest> phase2aRequests,
            List<PythonGrpcClient.ScreenshotRequestDTO> generatedRequests) {
        // 合并两路截图请求：做什么是保留 Phase2A 与生成结果；为什么是避免上游召回被忽略；权衡是可能略增重复提取
        Map<String, JavaCVFFmpegService.ScreenshotRequest> merged = new LinkedHashMap<>();
        appendScreenshotRequests(merged, phase2aRequests);
        appendScreenshotRequestsFromDto(merged, generatedRequests);
        return new ArrayList<>(merged.values());
    }

    private void appendScreenshotRequests(
            Map<String, JavaCVFFmpegService.ScreenshotRequest> merged,
            List<PythonGrpcClient.ScreenshotRequest> requests) {
        if (requests == null) return;
        for (PythonGrpcClient.ScreenshotRequest req : requests) {
            if (req == null) continue;
            String key = buildScreenshotKey(req.screenshotId, req.semanticUnitId, req.timestampSec);
            merged.computeIfAbsent(key, k -> new JavaCVFFmpegService.ScreenshotRequest(
                req.screenshotId, req.timestampSec, req.label, req.semanticUnitId
            ));
        }
    }

    private void appendScreenshotRequestsFromDto(
            Map<String, JavaCVFFmpegService.ScreenshotRequest> merged,
            List<PythonGrpcClient.ScreenshotRequestDTO> requests) {
        if (requests == null) return;
        for (PythonGrpcClient.ScreenshotRequestDTO req : requests) {
            if (req == null) continue;
            String key = buildScreenshotKey(req.screenshotId, req.semanticUnitId, req.timestampSec);
            merged.computeIfAbsent(key, k -> new JavaCVFFmpegService.ScreenshotRequest(
                req.screenshotId, req.timestampSec, req.label, req.semanticUnitId
            ));
        }
    }

    private String buildScreenshotKey(String screenshotId, String semanticUnitId, double timestampSec) {
        String id = screenshotId != null ? screenshotId.trim() : "";
        if (!id.isEmpty()) {
            return "id:" + id;
        }
        String unit = semanticUnitId != null ? semanticUnitId.trim() : "";
        return "ts:" + unit + "|" + Double.toString(timestampSec);
    }

    private List<JavaCVFFmpegService.ClipRequest> mergeClipRequests(
            List<PythonGrpcClient.ClipRequest> phase2aRequests,
            List<PythonGrpcClient.ClipRequestDTO> generatedRequests) {
        // 合并两路切片请求：做什么是保留 Phase2A 与生成结果；为什么是避免素材断链；权衡是可能增加切片数量
        Map<String, JavaCVFFmpegService.ClipRequest> merged = new LinkedHashMap<>();
        appendClipRequests(merged, phase2aRequests);
        appendClipRequestsFromDto(merged, generatedRequests);
        return new ArrayList<>(merged.values());
    }

    private void appendClipRequests(
            Map<String, JavaCVFFmpegService.ClipRequest> merged,
            List<PythonGrpcClient.ClipRequest> requests) {
        if (requests == null) return;
        for (PythonGrpcClient.ClipRequest req : requests) {
            if (req == null) continue;
            String key = buildClipKey(req.clipId, req.semanticUnitId, req.startSec, req.endSec);
            merged.computeIfAbsent(key, k -> new JavaCVFFmpegService.ClipRequest(
                req.clipId, req.startSec, req.endSec, req.knowledgeType, req.semanticUnitId
            ));
        }
    }

    private void appendClipRequestsFromDto(
            Map<String, JavaCVFFmpegService.ClipRequest> merged,
            List<PythonGrpcClient.ClipRequestDTO> requests) {
        if (requests == null) return;
        for (PythonGrpcClient.ClipRequestDTO req : requests) {
            if (req == null) continue;
            String key = buildClipKey(req.clipId, req.semanticUnitId, req.startSec, req.endSec);
            merged.computeIfAbsent(key, k -> new JavaCVFFmpegService.ClipRequest(
                req.clipId, req.startSec, req.endSec, req.knowledgeType, req.semanticUnitId
            ));
        }
    }

    private String buildClipKey(String clipId, String semanticUnitId, double startSec, double endSec) {
        String id = clipId != null ? clipId.trim() : "";
        if (!id.isEmpty()) {
            return "id:" + id;
        }
        String unit = semanticUnitId != null ? semanticUnitId.trim() : "";
        return "range:" + unit + "|" + Double.toString(startSec) + "-" + Double.toString(endSec);
    }

    private void ensureActionIds(List<ActionSegmentResult> actions) {
        // 统一 action_id：做什么是补齐/去重编号；为什么是保证分类结果可回写；权衡是编号可能随排序变化
        if (actions == null || actions.isEmpty()) return;
        Set<Integer> used = new HashSet<>();
        int nextId = 1;
        for (ActionSegmentResult as : actions) {
            if (as == null) continue;
            if (as.id > 0 && !used.contains(as.id)) {
                used.add(as.id);
                continue;
            }
            while (used.contains(nextId)) {
                nextId++;
            }
            as.id = nextId;
            used.add(nextId);
            nextId++;
        }
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
    
    private boolean isCVTask(String kType) {
        if (kType == null) return false;
        String t = kType.toLowerCase();
        // CV Types: Process, Practical (heavy CV)
        // CF Types: Explanation, Abstract, Configuration, Deduction (light CF)
        return t.contains("process") || t.contains("practical") || t.contains("过程") || t.contains("实操");
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
            
            // 统一 action_id：做什么是为每个 action 分配稳定编号；为什么是保证分类结果能回写；权衡是编号依赖当前排序
            ensureActionIds(in.actionUnits);
            // ❌ Removed: Subtitle mapping - Classifier reads directly from Step 2
            
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
            
            // 优先使用 action_units 的知识类型：做什么是避免二次分类；为什么是与语义回写一致；权衡是依赖上游回写完整性
            List<Map<String, Object>> unitActions = (List<Map<String, Object>>) u.get("action_units");
            if (unitActions != null && !unitActions.isEmpty()) {
                // 短日志：定位 JSON -> Java 是否拿到 action_units.knowledge_type
                Object firstKt = unitActions.get(0).get("knowledge_type");
                logger.info("[{}] MatInputs from semantic_units: unit={}, actions={}, first_kt={}",
                    "MaterialGen", uid, unitActions.size(), firstKt);
                for (Map<String, Object> au : unitActions) {
                    ActionSegmentResult as = new ActionSegmentResult();
                    as.id = parseInt(au.get("id"), 0);
                    as.startSec = parseDouble(au.get("start_sec"), 0.0);
                    as.endSec = parseDouble(au.get("end_sec"), 0.0);
                    String kt = au.get("knowledge_type") != null ? au.get("knowledge_type").toString() : "";
                    String fallback = in.knowledgeType != null ? in.knowledgeType : "";
                    // 不再使用 action_type 兜底：做什么是避免“knowledge”误当知识类型；为什么是保证讲解型过滤生效；权衡是依赖 unit 级兜底
                    as.actionType = !kt.isEmpty() ? kt : fallback;
                    in.actionUnits.add(as);
                }
            } else if (cvResults.containsKey(uid)) {
                logger.info("[{}] MatInputs fallback to CV actionSegments: unit={}, actions=0",
                    "MaterialGen", uid);
                // 兜底：没有 action_units 时，仍使用 CV 动作段，避免素材生成断链
                CVValidationUnitResult cvRes = cvResults.get(uid);
                if (cvRes.actionSegments != null) {
                    in.actionUnits.addAll(cvRes.actionSegments);
                }
            }

            // 关键修复: 传递稳定岛数据（用于截图范围）
            if (cvResults.containsKey(uid)) {
                CVValidationUnitResult cvRes = cvResults.get(uid);
                if (cvRes.stableIslands != null) {
                    in.stableIslands.addAll(cvRes.stableIslands);
                }
            }
            // 统一 action_id：做什么是保证下游一致性；为什么是便于跨阶段追踪；权衡是编号依赖当前排序
            ensureActionIds(in.actionUnits);
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
            // 🚀 V7.8: Do NOT overwrite Unit-Level knowledge_type with Action-Level classification.
            // The LLM results are specific to individual actions (e.g. "Explainer" action within "Process" unit).
            // We should trust the Unit Type from Stage 1 (Segmentation) or explicit Unit classification (if added later).
            // if (classMap.containsKey(uid)) { ... } // REMOVED
            
            // 1. Update CV results (Sync structure with Python expectation)
            if (cvResults.containsKey(uid)) {
                CVValidationUnitResult cvRes = cvResults.get(uid);
                unit.put("cv_validated", true);

                
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

    private int parseInt(Object val, int defaultVal) {
        if (val == null) return defaultVal;
        if (val instanceof Number) return ((Number) val).intValue();
        try {
            return Integer.parseInt(val.toString());
        } catch (Exception e) {
            return defaultVal;
        }
    }

    private void saveUpdatedSemantics(File file, Object root) {
        try {
            // 🚀 Pretty Print for better readability
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(file, root);
        } catch(IOException e) {
            logger.error("Failed to save updated semantics", e);
        }
    }
    
    // ❌ Removed: enrichUnitsWithSubtitles method
    // Classifier now reads subtitles directly from step2_path
    
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
