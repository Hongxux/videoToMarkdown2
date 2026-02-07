package com.mvp.module2.fusion.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * FFmpeg 服务
 * 
 * 🔑 Java负责执行FFmpeg操作（发挥并发优势）
 * 
 * 功能：
 * 1. 并行截取多张截图
 * 2. 并行切割多个视频片段
 * 3. 支持动态线程池大小
 * 4. 自动查找ffmpeg路径
 */
@Service
public class FFmpegService {
    
    private static final Logger logger = LoggerFactory.getLogger(FFmpegService.class);
    
    @Value("${ffmpeg.path:}")
    private String configuredFfmpegPath;
    
    @Value("${ffmpeg.threads:4}")
    private int ffmpegThreads;
    
    private String ffmpegPath;
    private ExecutorService executorService;
    
    // 常见的FFmpeg安装路径（Windows/Linux/Mac）
    private static final String[] COMMON_FFMPEG_PATHS = {
        // Windows Anaconda/Conda 环境
        "D:\\New_ANACONDA\\envs\\whisper_env\\Library\\bin\\ffmpeg.exe",
        "C:\\Anaconda3\\Library\\bin\\ffmpeg.exe",
        "C:\\ProgramData\\Anaconda3\\Library\\bin\\ffmpeg.exe",
        // Windows 常规安装
        "C:\\ffmpeg\\bin\\ffmpeg.exe",
        "C:\\Program Files\\ffmpeg\\bin\\ffmpeg.exe",
        // Linux/Mac
        "/usr/bin/ffmpeg",
        "/usr/local/bin/ffmpeg",
        "/opt/homebrew/bin/ffmpeg"
    };
    
    public FFmpegService() {
        // 使用固定线程池执行FFmpeg任务
        this.executorService = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
        );
    }
    
    /**
     * 初始化时自动查找FFmpeg路径
     */
    @jakarta.annotation.PostConstruct
    public void init() {
        this.ffmpegPath = findFfmpegPath();
        logger.info("FFmpegService initialized with path: {}", this.ffmpegPath);
    }
    
    /**
     * 查找FFmpeg路径（模拟Python的shutil.which逻辑）
     */
    private String findFfmpegPath() {
        // 1. 优先使用配置的路径
        if (configuredFfmpegPath != null && !configuredFfmpegPath.isEmpty()) {
            File configured = new File(configuredFfmpegPath);
            if (configured.exists() && configured.canExecute()) {
                logger.info("Using configured FFmpeg path: {}", configuredFfmpegPath);
                return configuredFfmpegPath;
            }
        }
        
        // 2. 尝试系统PATH（类似shutil.which）
        String osName = System.getProperty("os.name").toLowerCase();
        String ffmpegCmd = osName.contains("win") ? "ffmpeg.exe" : "ffmpeg";
        
        String pathEnv = System.getenv("PATH");
        if (pathEnv != null) {
            String pathSeparator = osName.contains("win") ? ";" : ":";
            for (String dir : pathEnv.split(pathSeparator)) {
                File ffmpegFile = new File(dir, ffmpegCmd);
                if (ffmpegFile.exists() && ffmpegFile.canExecute()) {
                    logger.info("Found FFmpeg in PATH: {}", ffmpegFile.getAbsolutePath());
                    return ffmpegFile.getAbsolutePath();
                }
            }
        }
        
        // 3. 尝试常见安装路径
        for (String path : COMMON_FFMPEG_PATHS) {
            File ffmpegFile = new File(path);
            if (ffmpegFile.exists() && ffmpegFile.canExecute()) {
                logger.info("Found FFmpeg at common path: {}", path);
                return path;
            }
        }
        
        // 4. 回退：假设在PATH中（让ProcessBuilder尝试）
        logger.warn("FFmpeg not found in common paths, using 'ffmpeg' command directly");
        return "ffmpeg";
    }
    
    /**
     * 截图请求
     */
    public static class ScreenshotRequest {
        public String screenshotId;
        public double timestampSec;
        public String label;
        public String semanticUnitId;
        
        public ScreenshotRequest(String screenshotId, double timestampSec, String label, String semanticUnitId) {
            this.screenshotId = screenshotId;
            this.timestampSec = timestampSec;
            this.label = label;
            this.semanticUnitId = semanticUnitId;
        }
    }
    
    /**
     * 切片请求
     */
    public static class ClipRequest {
        public String clipId;
        public double startSec;
        public double endSec;
        public String knowledgeType;
        public String semanticUnitId;
        
        public ClipRequest(String clipId, double startSec, double endSec, String knowledgeType, String semanticUnitId) {
            this.clipId = clipId;
            this.startSec = startSec;
            this.endSec = endSec;
            this.knowledgeType = knowledgeType;
            this.semanticUnitId = semanticUnitId;
        }
    }
    
    /**
     * 提取结果
     */
    public static class ExtractionResult {
        public String screenshotsDir;
        public String clipsDir;
        public int successfulScreenshots;
        public int successfulClips;
        public List<String> errors;
        
        public ExtractionResult() {
            this.errors = new ArrayList<>();
        }
    }
    
    /**
     * 🔑 并行执行所有FFmpeg操作
     * 
     * @param videoPath 视频文件路径
     * @param outputDir 输出目录
     * @param screenshotRequests 截图请求列表
     * @param clipRequests 切片请求列表
     * @param timeoutSeconds 超时时间（秒）
     * @return 提取结果
     */
    public CompletableFuture<ExtractionResult> extractAllAsync(
            String videoPath,
            String outputDir,
            List<ScreenshotRequest> screenshotRequests,
            List<ClipRequest> clipRequests,
            int timeoutSeconds
    ) {
        return CompletableFuture.supplyAsync(() -> {
            ExtractionResult result = new ExtractionResult();
            
            try {
                // 创建输出目录
                String assetsDir = Paths.get(outputDir, "assets").toString();

                Files.createDirectories(Paths.get(assetsDir));

                result.screenshotsDir = assetsDir;
                result.clipsDir = assetsDir;
                
                logger.info("Starting FFmpeg extraction: {} screenshots, {} clips",
                    screenshotRequests.size(), clipRequests.size());
                
                // 🔑 并行执行截图
                List<CompletableFuture<Boolean>> screenshotFutures = screenshotRequests.stream()
                    .map(req -> extractScreenshotAsync(videoPath, assetsDir, req))
                    .collect(Collectors.toList());
                
                // 🔑 并行执行切片
                List<CompletableFuture<Boolean>> clipFutures = clipRequests.stream()
                    .map(req -> extractClipAsync(videoPath, assetsDir, req))
                    .collect(Collectors.toList());
                
                // 等待所有截图完成
                CompletableFuture.allOf(screenshotFutures.toArray(new CompletableFuture[0]))
                    .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                    .join();
                
                // 等待所有切片完成
                CompletableFuture.allOf(clipFutures.toArray(new CompletableFuture[0]))
                    .orTimeout(timeoutSeconds, TimeUnit.SECONDS)
                    .join();
                
                // 统计结果
                result.successfulScreenshots = (int) screenshotFutures.stream()
                    .filter(f -> f.join())
                    .count();
                
                result.successfulClips = (int) clipFutures.stream()
                    .filter(f -> f.join())
                    .count();
                
                logger.info("FFmpeg extraction completed: {}/{} screenshots, {}/{} clips",
                    result.successfulScreenshots, screenshotRequests.size(),
                    result.successfulClips, clipRequests.size());
                
            } catch (Exception e) {
                logger.error("FFmpeg extraction failed", e);
                result.errors.add(e.getMessage());
            }
            
            return result;
        }, executorService);
    }
    
    /**
     * 异步提取单张截图
     */
    private CompletableFuture<Boolean> extractScreenshotAsync(
            String videoPath, String outputDir, ScreenshotRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path outputPath = Paths.get(outputDir, request.screenshotId + ".png");
                Path parentDir = outputPath.getParent();
                if (parentDir != null) {
                    Files.createDirectories(parentDir);
                }
                
                // 构建FFmpeg命令
                // ffmpeg -ss <timestamp> -i <video> -frames:v 1 -q:v 2 <output>
                ProcessBuilder pb = new ProcessBuilder(
                    ffmpegPath,
                    "-ss", String.format("%.3f", request.timestampSec),
                    "-i", videoPath,
                    "-frames:v", "1",
                    "-q:v", "2",
                    "-y",  // 覆盖已存在文件
                    outputPath.toString()
                );
                
                pb.redirectErrorStream(true);
                Process process = pb.start();
                
                // 读取输出
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        // 可选：记录FFmpeg输出
                    }
                }
                
                boolean success = process.waitFor(30, TimeUnit.SECONDS);
                if (success && process.exitValue() == 0) {
                    logger.debug("Screenshot extracted: {}", request.screenshotId);
                    return true;
                } else {
                    logger.warn("Screenshot failed: {} (exit={})", 
                        request.screenshotId, process.exitValue());
                    return false;
                }
                
            } catch (Exception e) {
                logger.error("Screenshot error: " + request.screenshotId, e);
                return false;
            }
        }, executorService);
    }
    
    /**
     * 异步提取单个视频片段
     */
    private CompletableFuture<Boolean> extractClipAsync(
            String videoPath, String outputDir, ClipRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path outputPath = Paths.get(outputDir, request.clipId + ".mp4");
                Path parentDir = outputPath.getParent();
                if (parentDir != null) {
                    Files.createDirectories(parentDir);
                }
                double duration = request.endSec - request.startSec;
                
                // 构建FFmpeg命令
                // ffmpeg -ss <start> -i <video> -t <duration> -c:v libx264 -crf 23 <output>
                ProcessBuilder pb = new ProcessBuilder(
                    ffmpegPath,
                    "-ss", String.format("%.3f", request.startSec),
                    "-i", videoPath,
                    "-t", String.format("%.3f", duration),
                    "-c:v", "libx264",
                    "-crf", "23",
                    "-c:a", "aac",
                    "-y",
                    outputPath.toString()
                );
                
                pb.redirectErrorStream(true);
                Process process = pb.start();
                
                // 读取输出
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        // 可选：记录FFmpeg输出
                    }
                }
                
                // 视频切片需要更长超时
                boolean success = process.waitFor(120, TimeUnit.SECONDS);
                if (success && process.exitValue() == 0) {
                    logger.debug("Clip extracted: {} ({:.1f}s)", request.clipId, duration);
                    return true;
                } else {
                    logger.warn("Clip failed: {} (exit={})", 
                        request.clipId, process.exitValue());
                    return false;
                }
                
            } catch (Exception e) {
                logger.error("Clip error: " + request.clipId, e);
                return false;
            }
        }, executorService);
    }
    
    /**
     * 同步执行所有FFmpeg操作
     */
    public ExtractionResult extractAllSync(
            String videoPath,
            String outputDir,
            List<ScreenshotRequest> screenshotRequests,
            List<ClipRequest> clipRequests,
            int timeoutSeconds
    ) {
        return extractAllAsync(videoPath, outputDir, screenshotRequests, clipRequests, timeoutSeconds)
            .join();
    }
    
    /**
     * 关闭线程池
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }
}
