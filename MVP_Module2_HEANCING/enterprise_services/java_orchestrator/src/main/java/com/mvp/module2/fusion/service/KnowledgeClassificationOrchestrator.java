package com.mvp.module2.fusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mvp.module2.fusion.grpc.PythonGrpcClient;
import com.mvp.module2.fusion.grpc.PythonGrpcClient.*;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/**
 * 知识分类并发编排器
 * 
 * 🚀 Strategy:
 * 1. Token-Aware Batching: 动态打包 (Count >= 5 or Chars >= 4000)
 * 2. IO Pool Concurrency: 使用高并发池调用 Python
 * 3. Circuit Breaking: 保护 LLM API (429/5xx)
 * 4. Safe Parsing: 防止由于 JSON 字段缺失导致的 NPE
 */
@Service
public class KnowledgeClassificationOrchestrator {
    
    private static final Logger logger = LoggerFactory.getLogger(KnowledgeClassificationOrchestrator.class);
    
    @Autowired
    private PythonGrpcClient grpcClient;
    
    @Autowired
    private AdaptiveResourceOrchestrator adaptiveOrch;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    // Circuit Breaker for LLM
    private final CircuitBreaker circuitBreaker;
    
    public KnowledgeClassificationOrchestrator() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .slidingWindowSize(10)
            .build();
        this.circuitBreaker = CircuitBreakerRegistry.of(config).circuitBreaker("llmClassification");
    }
    
    /**
     * 执行并行分类
     * @param units 包含 CV 结果的语义单元
     * @param step2Path Step 2 字幕文件路径
     * @param outputDir 输出目录
     */
    public List<KnowledgeResultItem> classifyParallel(String taskId, List<ClassificationInput> units, String step2Path, String outputDir) {
        // 0. Cache Check
        String cachePath = outputDir + "/intermediates/modality_classification_cache.json";
        List<KnowledgeResultItem> cachedResults = loadFromCache(taskId, cachePath);
        if (cachedResults != null) {
            logger.info("[{}] ✅ Reusing cached knowledge classification results", taskId);
            return cachedResults;
        }

        if (units == null || units.isEmpty()) return new ArrayList<>();

        List<List<ClassificationInput>> batches = createTokenAwareBatches(units);
        logger.info("[{}] Start Knowledge Classification: {} units -> {} batches", taskId, units.size(), batches.size());
        
        List<CompletableFuture<ClassificationBatchResult>> futures = new ArrayList<>();
        for (List<ClassificationInput> batch : batches) {
            futures.add(classifyBatchAsync(taskId, batch, step2Path));
        }
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        
        // Merge results
        List<KnowledgeResultItem> allResults = new ArrayList<>();
        int successCount = 0;
        
        for (CompletableFuture<ClassificationBatchResult> f : futures) {
            try {
                ClassificationBatchResult batchRes = f.get();
                if (batchRes.success && batchRes.results != null) {
                    allResults.addAll(batchRes.results);
                    successCount++;
                }
            } catch (Exception e) {
                logger.error("[{}] Future get error: {}", taskId, e.getMessage());
            }
        }
        
        logger.info("[{}] Classification complete. Success batches: {}/{}. Total results: {}", 
            taskId, successCount, batches.size(), allResults.size());
            
        saveToCache(taskId, cachePath, allResults);
        return allResults;
    }

    public CompletableFuture<ClassificationBatchResult> classifyBatchAsync(String taskId, List<ClassificationInput> batch, String step2Path) {
        Semaphore semaphore = adaptiveOrch.getIOSemaphore();
        return CompletableFuture.supplyAsync(() -> {
            try {
                semaphore.acquire();
                return circuitBreaker.executeSupplier(() -> {
                    try {
                        // Using 600s timeout as per robustness tuning
                        return grpcClient.classifyKnowledgeBatchAsync(taskId, batch, step2Path, 600).join();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (Exception e) {
                logger.error("[{}] Batch classification failed: {}", taskId, e.getMessage());
                ClassificationBatchResult failRes = new ClassificationBatchResult();
                failRes.success = false;
                failRes.errorMsg = e.getMessage();
                return failRes;
            } finally {
                semaphore.release();
            }
        });
    }
    
    private List<KnowledgeResultItem> loadFromCache(String taskId, String path) {
        File file = new File(path);
        if (!file.exists()) return null;
        try {
            Map<String, Object> data = objectMapper.readValue(file, Map.class);
            if (data.containsKey("units")) {
                List<Map<String, Object>> units = (List<Map<String, Object>>) data.get("units");
                return units.stream().map(u -> {
                    KnowledgeResultItem item = new KnowledgeResultItem();
                    item.unitId = (String) u.get("unit_id");
                    item.actionId = parseInt(u.getOrDefault("action_id", 0), 0);
                    item.knowledgeType = (String) u.get("knowledge_type");
                    item.confidence = parseDouble(u.get("confidence"), 1.0);
                    item.keyEvidence = (String) u.get("key_evidence");
                    item.reasoning = (String) u.get("reasoning");
                    return item;
                }).collect(Collectors.toList());
            }
        } catch (Exception e) {
            logger.warn("[{}] Failed to load classification cache: {}", taskId, e.getMessage());
        }
        return null;
    }

    private void saveToCache(String taskId, String path, List<KnowledgeResultItem> results) {
        try {
            File file = new File(path);
            file.getParentFile().mkdirs();
            Map<String, Object> data = new HashMap<>();
            data.put("units", results);
            objectMapper.writeValue(file, data);
            logger.info("[{}] ✅ Knowledge classification results saved to cache: {}", taskId, path);
        } catch (Exception e) {
            logger.warn("[{}] Failed to save classification cache: {}", taskId, e.getMessage());
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
    
    public List<List<ClassificationInput>> createTokenAwareBatches(List<ClassificationInput> units) {
        List<List<ClassificationInput>> batches = new ArrayList<>();
        List<ClassificationInput> currentBatch = new ArrayList<>();
        int currentChars = 0;
        
        for (ClassificationInput unit : units) {
            int unitChars = (unit.title != null ? unit.title.length() : 0) + 
                            (unit.text != null ? unit.text.length() : 0);
            
            if (!currentBatch.isEmpty()) {
                boolean countLimit = currentBatch.size() >= 5;
                boolean sizeLimit = (currentChars + unitChars) >= 4000;
                
                if (countLimit || sizeLimit) {
                    batches.add(new ArrayList<>(currentBatch));
                    currentBatch.clear();
                    currentChars = 0;
                }
            }
            
            currentBatch.add(unit);
            currentChars += unitChars;
        }
        
        if (!currentBatch.isEmpty()) {
            batches.add(currentBatch);
        }
        
        return batches;
    }
}
