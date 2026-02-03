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
    
    // 每批次处理的单元数 (设为1实现细粒度控制)
    private static final int DEFAULT_BATCH_SIZE = 1;
    // 单批次超时秒数
    private static final int BATCH_TIMEOUT_SEC = 6000;  // 单个 unit 最多 10 分钟

    
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
        
        List<CompletableFuture<CVBatchResult>> futures = validateBatchesAsync(taskId, videoPath, units, outputDir);
        if (futures == null) return Collections.emptyMap();

        Map<String, CVValidationUnitResult> resultMap = new ConcurrentHashMap<>();
        try {
            CompletableFuture<Void> allFutures = CompletableFuture.allOf(
                futures.toArray(new CompletableFuture[0])
            );
            
            allFutures.get(BATCH_TIMEOUT_SEC * Math.max(1, futures.size()), TimeUnit.SECONDS);
            
            for (CompletableFuture<CVBatchResult> future : futures) {
                CVBatchResult batchResult = future.get();
                if (batchResult.success && batchResult.results != null) {
                    for (CVValidationUnitResult unitResult : batchResult.results) {
                        resultMap.put(unitResult.unitId, unitResult);
                    }
                }
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
     */
    public List<CompletableFuture<CVBatchResult>> validateBatchesAsync(
            String taskId, String videoPath, List<SemanticUnitInput> units, String outputDir) {
        
        if (units == null || units.isEmpty()) return null;

        // 0. Cache Check (Simplified for batching context)
        // Note: For now, if anyone calls the full parallel version, cache is checked there.
        // For pipelining, we might want to check cache per batch or globally.
        // Let's stick to global check for now to keep it simple.

        int adaptiveLimit = adaptiveOrchestrator.getAdaptiveLimit();
        int batchSize = Math.max(1, DEFAULT_BATCH_SIZE * adaptiveLimit / 2);
        List<List<SemanticUnitInput>> batches = partition(units, batchSize);
        
        List<CompletableFuture<CVBatchResult>> futures = new ArrayList<>();
        Semaphore semaphore = adaptiveOrchestrator.getComputeSemaphore();
        
        for (int i = 0; i < batches.size(); i++) {
            final int batchIndex = i;
            final List<SemanticUnitInput> batch = batches.get(i);
            
            CompletableFuture<CVBatchResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    semaphore.acquire();
                    try {
                        return executeWithResilience(taskId, videoPath, batch, batchIndex);
                    } finally {
                        semaphore.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    CVBatchResult failResult = new CVBatchResult();
                    failResult.success = false;
                    failResult.errorMsg = "Interrupted";
                    return failResult;
                }
            }, executor);
            futures.add(future);
        }
        return futures;
    }
    
    private Map<String, CVValidationUnitResult> loadFromCache(String taskId, String path) {
        File file = new File(path);
        if (!file.exists()) return null;
        try {
            return objectMapper.readValue(file, new com.fasterxml.jackson.core.type.TypeReference<Map<String, CVValidationUnitResult>>() {});
        } catch (Exception e) {
            logger.warn("[{}] Failed to load CV cache: {}", taskId, e.getMessage());
        }
        return null;
    }

    private void saveToCache(String taskId, String path, Map<String, CVValidationUnitResult> results) {
        try {
            File file = new File(path);
            file.getParentFile().mkdirs();
            objectMapper.writeValue(file, results);
            logger.info("[{}] ✅ CV results saved to cache: {}", taskId, path);
        } catch (Exception e) {
            logger.warn("[{}] Failed to save CV cache: {}", taskId, e.getMessage());
        }
    }
    
    /**
     * 使用熔断器和重试执行单批次验证
     */
    private CVBatchResult executeWithResilience(
            String taskId, String videoPath, 
            List<SemanticUnitInput> batch, int batchIndex) {
        
        try {
            // 包装调用：熔断器 -> 重试 -> 实际调用
            return circuitBreaker.executeSupplier(
                Retry.decorateSupplier(retry, () -> {
                    try {
                        return grpcClient.validateCVBatchAsync(
                            taskId, videoPath, batch, BATCH_TIMEOUT_SEC
                        ).get(BATCH_TIMEOUT_SEC, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        throw new RuntimeException("gRPC call failed", e);
                    }
                })
            );
        } catch (Exception e) {
            logger.error("[{}] Batch {} failed after retries: {}", 
                taskId, batchIndex, e.getMessage());
            CVBatchResult failResult = new CVBatchResult();
            failResult.success = false;
            failResult.errorMsg = e.getMessage();
            return failResult;
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
