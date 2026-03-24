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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
    private static final String CLASS_CACHE_VERSION = "llm_v1";
    private static final String CLASS_CACHE_FILE = "modality_classification_cache.json";
    
    @Autowired
    private PythonGrpcClient grpcClient;
    
    @Autowired
    private AdaptiveResourceOrchestrator adaptiveOrch;
    
    @Autowired
    private ObjectMapper objectMapper;
    
    // Circuit Breaker for LLM
    private final CircuitBreaker circuitBreaker;
    
    // 🚀 Dedicated IO Executor to prevent ForkJoinPool starvation
    private final java.util.concurrent.ExecutorService ioExecutor;
    
    public KnowledgeClassificationOrchestrator() {
        CircuitBreakerConfig config = CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .waitDurationInOpenState(Duration.ofSeconds(10))
            .slidingWindowSize(10)
            .build();
        this.circuitBreaker = CircuitBreakerRegistry.of(config).circuitBreaker("llmClassification");
        
        // Use a cached pool for IO-bound blocking gRPC calls
        this.ioExecutor = java.util.concurrent.Executors.newCachedThreadPool();
    }
    
    /**
     * 执行并行分类
     * @param units 包含 CV 结果的语义单元
     * @param step2Path Step 2 字幕文件路径
     * @param outputDir 输出目录
     */
    public List<KnowledgeResultItem> classifyParallel(String taskId, List<ClassificationInput> units, String step2Path, String outputDir) {
        // 0. Cache Check
        List<KnowledgeResultItem> cachedResults = tryLoadCachedResults(taskId, units, step2Path, outputDir);
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
            
        saveCache(taskId, allResults, units, step2Path, outputDir);
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
                        return grpcClient.classifyKnowledgeBatch(taskId, batch, step2Path, 600);
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
        }, ioExecutor);
    }
    
    public List<KnowledgeResultItem> tryLoadCachedResults(
            String taskId, List<ClassificationInput> units, String step2Path, String outputDir) {
        String cachePath = outputDir + File.separator + "intermediates" + File.separator + CLASS_CACHE_FILE;
        String signature = buildSignature(units, step2Path, outputDir);
        return loadFromCache(taskId, cachePath, signature);
    }

    public void saveCache(
            String taskId, List<KnowledgeResultItem> results,
            List<ClassificationInput> units, String step2Path, String outputDir) {
        if (results == null || results.isEmpty()) return;
        String cachePath = outputDir + File.separator + "intermediates" + File.separator + CLASS_CACHE_FILE;
        String signature = buildSignature(units, step2Path, outputDir);
        saveToCache(taskId, cachePath, signature, results);
    }

    private String buildSignature(List<ClassificationInput> units, String step2Path, String outputDir) {
        try {
            File step2File = step2Path != null ? new File(step2Path) : null;
            long step2Size = (step2File != null && step2File.exists()) ? step2File.length() : -1;
            long step2Mtime = (step2File != null && step2File.exists()) ? step2File.lastModified() : -1;
            String urlHash = new File(outputDir).getName();

            List<Map<String, Object>> unitList = new ArrayList<>();
            if (units != null) {
                List<ClassificationInput> sorted = new ArrayList<>(units);
                sorted.sort(Comparator.comparing(u -> u.unitId));
                for (ClassificationInput u : sorted) {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("unit_id", u.unitId);
                    String textRaw = (u.title != null ? u.title : "") + "|" + (u.text != null ? u.text : "");
                    item.put("text_hash", sha256(textRaw));

                    List<Map<String, Object>> actions = new ArrayList<>();
                    if (u.actionUnits != null) {
                        for (ActionSegmentResult as : u.actionUnits) {
                            Map<String, Object> a = new LinkedHashMap<>();
                            a.put("start_sec", as.startSec);
                            a.put("end_sec", as.endSec);
                            a.put("id", as.id);
                            actions.add(a);
                        }
                    }
                    item.put("actions", actions);
                    unitList.add(item);
                }
            }

            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("url_hash", urlHash);
            payload.put("llm_version", CLASS_CACHE_VERSION);
            payload.put("step2_size", step2Size);
            payload.put("step2_mtime", step2Mtime);
            payload.put("units", unitList);

            String raw = objectMapper.writeValueAsString(payload);
            return sha256(raw);
        } catch (Exception e) {
            logger.warn("Failed to build classification signature: {}", e.getMessage());
            return "";
        }
    }

    private List<KnowledgeResultItem> loadFromCache(String taskId, String path, String signature) {
        File file = new File(path);
        if (!file.exists()) return null;
        try {
            Map<String, Object> data = objectMapper.readValue(file, Map.class);
            Map<String, Object> meta = (Map<String, Object>) data.get("meta");
            if (meta == null || signature.isEmpty()) return null;
            if (!signature.equals(meta.get("signature"))) return null;

            if (data.containsKey("units")) {
                List<Map<String, Object>> units = (List<Map<String, Object>>) data.get("units");
                // 兼容旧/新两种字段命名：
                // - saveToCache(objectMapper) 默认使用 camelCase：unitId/actionId/knowledgeType/keyEvidence
                // - 历史实现误按 snake_case 读取：unit_id/action_id/knowledge_type/key_evidence
                // 若读取不兼容会造成“缓存命中但结果为空字段”，进而导致 action_units.knowledge_type 断链。
                List<KnowledgeResultItem> parsed = units.stream().map(u -> {
                    KnowledgeResultItem item = new KnowledgeResultItem();
                    Object unitIdVal = u.containsKey("unit_id") ? u.get("unit_id") : u.get("unitId");
                    item.unitId = unitIdVal != null ? unitIdVal.toString() : "";

                    Object actionIdVal = u.containsKey("action_id") ? u.get("action_id") : u.getOrDefault("actionId", 0);
                    item.actionId = parseInt(actionIdVal, 0);

                    Object ktVal = u.containsKey("knowledge_type") ? u.get("knowledge_type") : u.get("knowledgeType");
                    item.knowledgeType = ktVal != null ? ktVal.toString() : "";
                    item.confidence = parseDouble(u.get("confidence"), 1.0);
                    Object evVal = u.containsKey("key_evidence") ? u.get("key_evidence") : u.get("keyEvidence");
                    item.keyEvidence = evVal != null ? evVal.toString() : "";
                    item.reasoning = (String) u.get("reasoning");
                    return item;
                }).collect(Collectors.toList());

                long valid = parsed.stream()
                    .filter(r -> r != null && r.unitId != null && !r.unitId.isEmpty()
                        && r.knowledgeType != null && !r.knowledgeType.isEmpty())
                    .count();
                if (valid == 0 && !parsed.isEmpty()) {
                    logger.warn("[{}] Classification cache seems invalid (field-name mismatch?), ignore: {}", taskId, path);
                    return null;
                }
                return parsed;
            }
        } catch (Exception e) {
            logger.warn("[{}] Failed to load classification cache: {}", taskId, e.getMessage());
        }
        return null;
    }

    private void saveToCache(String taskId, String path, String signature, List<KnowledgeResultItem> results) {
        try {
            File file = new File(path);
            file.getParentFile().mkdirs();
            Map<String, Object> data = new HashMap<>();
            Map<String, Object> meta = new HashMap<>();
            meta.put("signature", signature);
            meta.put("version", CLASS_CACHE_VERSION);
            meta.put("updated_at", System.currentTimeMillis());
            meta.put("units_count", results.size());
            data.put("meta", meta);
            data.put("units", results);
            objectMapper.writeValue(file, data);
            logger.info("[{}] Knowledge classification results saved to cache: {}", taskId, path);
        } catch (Exception e) {
            logger.warn("[{}] Failed to save classification cache: {}", taskId, e.getMessage());
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
