package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.grpc.PythonGrpcClient.*;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 🚀 CV验证并行编排器
 * 
 * 功能:
 * 1. 将语义单元拆分为多个批次
 * 2. 使用AdaptiveResourceOrchestrator控制并发
 * 3. 实现熔断机制(Circuit Breaker)
 * 4. 实现重试机制(Retry)
 * 5. 聚合所有批次结果
 */
@Component
public class CVValidationOrchestrator {
    private static final Logger logger = LoggerFactory.getLogger(CVValidationOrchestrator.class);
    
    @Autowired
    private PythonGrpcClient grpcClient;
    
    @Autowired
    private AdaptiveResourceOrchestrator adaptiveOrchestrator;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    private final ExecutorService executor;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    
    // 每批次处理的单元数 (强制设为1，配合流式响应实现最大化并行度)
    // ?????????(?? 4?????????????????)
    private static final int DEFAULT_BATCH_SIZE = 8;
    // 单批次超时秒数
    private static final int BATCH_TIMEOUT_SEC = 6000;  // 单个 unit 最多 10 分钟
    // 缓存版本（配置变更时手动升级）
    private static final String CV_CACHE_VERSION = "cv_v1";
    private static final String CV_CACHE_FILE = "cv_validation_cache.json";

    
    public CVValidationOrchestrator() {
        // 初始化线程池
        this.executor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
        );
        
        // 🔧 熔断器配置
        CircuitBreakerConfig cbConfig = CircuitBreakerConfig.custom()
            .failureRateThreshold(50)                    // 50%失败率触发熔断
            .slowCallRateThreshold(80)                   // 80%慢调用触发熔断
            .slowCallDurationThreshold(Duration.ofSeconds(60))  // 60秒算慢调用
            .waitDurationInOpenState(Duration.ofSeconds(30))   // 熔断后30秒尝试恢复
            .permittedNumberOfCallsInHalfOpenState(3)    // 半开状态允许3次调用
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(10)                       // 统计最近10次调用
            .build();
        this.circuitBreaker = CircuitBreaker.of("cv-validation", cbConfig);
        
        // 🔧 重试配置
        RetryConfig retryConfig = RetryConfig.custom()
            .maxAttempts(3)                              // 最多重试3次
            .waitDuration(Duration.ofSeconds(2))         // 重试间隔2秒
            .retryExceptions(Exception.class)            // 所有异常都重试
            .build();
        this.retry = Retry.of("cv-validation-retry", retryConfig);
        
        logger.info("CVValidationOrchestrator initialized with CircuitBreaker and Retry");
    }
    
    /**
     * 并行验证所有语义单元
     * 
     * @param taskId 任务ID
     * @param videoPath 视频路径
     * @param units 语义单元列表
     * @param outputDir 输出目录
     * @return 验证结果Map (unitId -> result)
     */
    public Map<String, CVValidationUnitResult> validateParallel(
            String taskId, 
            String videoPath, 
            List<SemanticUnitInput> units,
            String outputDir) {
        
        Map<String, CVValidationUnitResult> resultMap = new ConcurrentHashMap<>();
        Map<String, CVValidationUnitResult> cached = tryLoadCachedResults(taskId, videoPath, units, outputDir);
        if (cached != null && !cached.isEmpty()) {
            resultMap.putAll(cached);
            return resultMap;
        }
        
        List<CompletableFuture<Boolean>> futures = validateBatchesAsync(taskId, videoPath, units, outputDir, unitResult -> {
            resultMap.put(unitResult.unitId, unitResult);
        });
        if (futures == null) return Collections.emptyMap();
        try {
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
            );
            
            allFutures.get(BATCH_TIMEOUT_SEC * Math.max(1, futures.size()), TimeUnit.SECONDS);
            
            // Results are already populated in resultMap via the consumer
            if (units != null && !units.isEmpty() && resultMap.size() >= units.size()) {
                saveCache(taskId, videoPath, units, outputDir, resultMap);
            }
        } catch (Exception e) {
            logger.error("[{}] CV Validation failed: {}", taskId, e.getMessage());
        } finally {
            // 🚀 V6: Explicitly release Python CV resources
            try {
                grpcClient.releaseCVResourcesAsync(taskId)
                    .thenAccept(res -> {
                         if (res.success) {
                             logger.info("[{}] 🧹 CV Resources Released: {}", taskId, res.message);
                         } else {
                             logger.warn("[{}] ⚠️ CV Resource Release Warning: {}", taskId, res.message);
                         }
                    });
            } catch (Exception e) {
                 logger.warn("[{}] Failed to trigger CV resource release", taskId);
            }
        }
        return resultMap;
    }

    /**
     * 并行启动批次，返回 Future 列表以供流水线编排
     * 🚀 增强: 支持流式结果回调
     */
    public List<CompletableFuture<Boolean>> validateBatchesAsync(
            String taskId, String videoPath, List<SemanticUnitInput> units, 
            String outputDir, java.util.function.Consumer<CVValidationUnitResult> resultConsumer) {
        
        if (units == null || units.isEmpty()) return null;

        // 0. Cache Check (Simplified for batching context)
        // Note: For now, if anyone calls the full parallel version, cache is checked there.
        // For pipelining, we might want to check cache per batch or globally.
        // Let's stick to global check for now to keep it simple.

        int adaptiveLimit = adaptiveOrchestrator.getAdaptiveLimit();
        int batchSize = Math.max(1, DEFAULT_BATCH_SIZE * adaptiveLimit / 2);
        List<List<SemanticUnitInput>> batches = partition(units, batchSize);
        
        List<CompletableFuture<Boolean>> futures = new ArrayList<>();
        Semaphore semaphore = adaptiveOrchestrator.getComputeSemaphore();
        
        for (int i = 0; i < batches.size(); i++) {
            final int batchIndex = i;
            final List<SemanticUnitInput> batch = batches.get(i);
            
            CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                try {
                    semaphore.acquire();
                    try {
                        return executeWithResilienceStreaming(taskId, videoPath, batch, batchIndex, resultConsumer);
                    } finally {
                        semaphore.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }, executor);
            futures.add(future);
        }
        return futures;
    }
    
    public Map<String, CVValidationUnitResult> tryLoadCachedResults(
            String taskId, String videoPath, List<SemanticUnitInput> units, String outputDir) {
        String cachePath = getCvCachePath(outputDir);
        String signature = buildCvSignature(videoPath, units, outputDir);
        return loadFromCache(taskId, cachePath, signature, units != null ? units.size() : 0);
    }

    public void saveCache(
            String taskId, String videoPath, List<SemanticUnitInput> units, String outputDir,
            Map<String, CVValidationUnitResult> results) {
        if (results == null || results.isEmpty()) return;
        String cachePath = getCvCachePath(outputDir);
        String signature = buildCvSignature(videoPath, units, outputDir);
        saveToCache(taskId, cachePath, signature, results);
    }

    private String getCvCachePath(String outputDir) {
        return outputDir + File.separator + "intermediates" + File.separator + CV_CACHE_FILE;
    }

    private String buildCvSignature(String videoPath, List<SemanticUnitInput> units, String outputDir) {
        try {
            File videoFile = new File(videoPath);
            long videoSize = videoFile.exists() ? videoFile.length() : -1;
            long videoMtime = videoFile.exists() ? videoFile.lastModified() : -1;
            String urlHash = new File(outputDir).getName();

            List<Map<String, Object>> unitList = new ArrayList<>();
            if (units != null) {
                List<SemanticUnitInput> sorted = new ArrayList<>(units);
                sorted.sort(Comparator.comparing(u -> u.unitId));
                for (SemanticUnitInput u : sorted) {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("unit_id", u.unitId);
                    item.put("start_sec", u.startSec);
                    item.put("end_sec", u.endSec);
                    item.put("knowledge_type", u.knowledgeType != null ? u.knowledgeType : "");
                    unitList.add(item);
                }
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("url_hash", urlHash);
            payload.put("cv_version", CV_CACHE_VERSION);
            payload.put("video_size", videoSize);
            payload.put("video_mtime", videoMtime);
            payload.put("units", unitList);

            String raw = objectMapper.writeValueAsString(payload);
            return sha256(raw);
        } catch (Exception e) {
            logger.warn("Failed to build CV signature: {}", e.getMessage());
            return "";
        }
    }

    private Map<String, CVValidationUnitResult> loadFromCache(
            String taskId, String path, String signature, int expectedUnits) {
        File file = new File(path);
        if (!file.exists()) return null;
        try {
            Map<String, Object> root = objectMapper.readValue(file, Map.class);
            Map<String, Object> meta = (Map<String, Object>) root.get("meta");
            if (meta == null || signature.isEmpty()) return null;
            if (!signature.equals(meta.get("signature"))) return null;

            Map<String, CVValidationUnitResult> results = objectMapper.convertValue(
                root.get("results"),
                new com.fasterxml.jackson.core.type.TypeReference<Map<String, CVValidationUnitResult>>() {}
            );
            if (results == null || results.isEmpty()) return null;
            if (expectedUnits > 0 && results.size() < expectedUnits) {
                logger.warn("[{}] CV cache incomplete: {}/{}", taskId, results.size(), expectedUnits);
                return null;
            }
            logger.info("[{}] Reusing CV cache: {}", taskId, path);
            return results;
        } catch (Exception e) {
            logger.warn("[{}] Failed to load CV cache: {}", taskId, e.getMessage());
        }
        return null;
    }

    private void saveToCache(String taskId, String path, String signature, Map<String, CVValidationUnitResult> results) {
        try {
            File file = new File(path);
            file.getParentFile().mkdirs();

            Map<String, Object> meta = new LinkedHashMap<>();
            meta.put("signature", signature);
            meta.put("version", CV_CACHE_VERSION);
            meta.put("updated_at", System.currentTimeMillis());
            meta.put("units_count", results.size());

            Map<String, Object> root = new LinkedHashMap<>();
            root.put("meta", meta);
            root.put("results", results);

            objectMapper.writeValue(file, root);
            logger.info("[{}] CV results saved to cache: {}", taskId, path);
        } catch (Exception e) {
            logger.warn("[{}] Failed to save CV cache: {}", taskId, e.getMessage());
        }
    }

    private String sha256(String raw) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] bytes = md.digest(raw.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

/**
     * 使用熔断器和重试执行单批次流式验证
     */
    private boolean executeWithResilienceStreaming(
            String taskId, String videoPath, 
            List<SemanticUnitInput> batch, int batchIndex,
            java.util.function.Consumer<CVValidationUnitResult> resultConsumer) {
        
        try {
            return circuitBreaker.executeSupplier(
                Retry.decorateSupplier(retry, () -> {
                    try {
                        return grpcClient.validateCVBatchStreaming(
                            taskId, videoPath, batch, BATCH_TIMEOUT_SEC, resultConsumer
                        ).get(BATCH_TIMEOUT_SEC, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new RuntimeException("gRPC streaming call failed", e);
                    }
                })
            );
        } catch (Exception e) {
            logger.error("[{}] Streaming Batch {} failed after retries: {}", 
                taskId, batchIndex, e.getMessage());
            return false;
        }
    }
    
    /**
     * 将列表分割为指定大小的批次
     */
    private <T> List<List<T>> partition(List<T> list, int size) {
        List<List<T>> partitions = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            partitions.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return partitions;
    }
}
