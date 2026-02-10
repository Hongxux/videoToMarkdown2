package com.mvp.module2.fusion.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.MemoryMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.mvp.module2.fusion.service.AdaptiveResourceOrchestrator;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 系统负载资源调度器
 * 
 * 功能：
 * 1. 监控 CPU/内存使用率
 * 2. 动态调整并发任务数
 * 3. 负载过高时自动限流
 */
@Component
public class LoadBasedScheduler {
    
    private static final Logger logger = LoggerFactory.getLogger(LoadBasedScheduler.class);

    @Autowired
    private AdaptiveResourceOrchestrator adaptiveOrchestrator;
    
    // 阈值配置
    private static final double CPU_HIGH_THRESHOLD = 80.0;
    private static final double CPU_LOW_THRESHOLD = 50.0;
    private static final double MEMORY_HIGH_THRESHOLD = 85.0;
    private static final long MIN_AVAILABLE_MEMORY_MB = 512;
    
    // 并发控制
    private static final int MIN_CONCURRENT_TASKS = 1;
    private static final int MAX_CONCURRENT_TASKS = 8;
    private final AtomicInteger currentMaxConcurrent = new AtomicInteger(4);
    
    // 系统监控
    private final OperatingSystemMXBean osMXBean;
    private final MemoryMXBean memoryMXBean;
    private final ScheduledExecutorService scheduler;
    
    // 当前系统状态
    private volatile double currentCpuLoad = 0.0;
    private volatile double currentMemoryUsage = 0.0;
    private volatile long availableMemoryMB = 0;
    private volatile SystemState systemState = SystemState.NORMAL;
    
    public enum SystemState {
        NORMAL,     // 正常
        BUSY,       // 繁忙（限制新任务）
        OVERLOADED  // 过载（拒绝新任务）
    }
    
    public LoadBasedScheduler() {
        this.osMXBean = ManagementFactory.getOperatingSystemMXBean();
        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }
    
    @PostConstruct
    public void init() {
        // 每5秒更新系统指标
        scheduler.scheduleAtFixedRate(this::updateSystemMetrics, 0, 5, TimeUnit.SECONDS);
        logger.info("LoadBasedScheduler initialized");
    }
    
    /**
     * 更新系统指标
     */
    private void updateSystemMetrics() {
        try {
            // CPU负载
            currentCpuLoad = osMXBean.getSystemLoadAverage();
            if (currentCpuLoad < 0) {
                // Windows不支持getSystemLoadAverage，使用替代方案
                currentCpuLoad = estimateCpuLoad();
            }
            
            // 内存使用率
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long maxMemory = runtime.maxMemory();
            
            long usedMemory = totalMemory - freeMemory;
            currentMemoryUsage = (double) usedMemory / maxMemory * 100;
            availableMemoryMB = (maxMemory - usedMemory) / (1024 * 1024);
            
            // 更新系统状态
            updateSystemState();
            
            // 动态调整并发数
            adjustConcurrency();
            
            // 🚀 驱动 AdaptiveResourceOrchestrator 进行更精细的 Semaphore 调整
            if (adaptiveOrchestrator != null) {
                adaptiveOrchestrator.adjustConcurrency();
            }
            
        } catch (Exception e) {
            logger.warn("Failed to update system metrics: {}", e.getMessage());
        }
    }
    
    /**
     * Windows下估算CPU负载
     */
    private double estimateCpuLoad() {
        try {
            if (osMXBean instanceof com.sun.management.OperatingSystemMXBean sunOsMXBean) {
                return sunOsMXBean.getCpuLoad() * 100;
            }
        } catch (Exception e) {
            // ignore
        }
        return 50.0; // 默认中等负载
    }
    
    /**
     * 更新系统状态
     */
    private void updateSystemState() {
        SystemState previousState = systemState;
        
        if (currentCpuLoad > CPU_HIGH_THRESHOLD || 
            currentMemoryUsage > MEMORY_HIGH_THRESHOLD ||
            availableMemoryMB < MIN_AVAILABLE_MEMORY_MB) {
            systemState = SystemState.OVERLOADED;
        } else if (currentCpuLoad > CPU_LOW_THRESHOLD) {
            systemState = SystemState.BUSY;
        } else {
            systemState = SystemState.NORMAL;
        }
        
        if (systemState != previousState) {
            logger.info("System state changed: {} -> {} (CPU={:.1f}%, Mem={:.1f}%, Free={}MB)",
                previousState, systemState, currentCpuLoad, currentMemoryUsage, availableMemoryMB);
        }
    }
    
    /**
     * 动态调整并发数
     */
    private void adjustConcurrency() {
        int current = currentMaxConcurrent.get();
        int newValue = current;
        
        switch (systemState) {
            case NORMAL:
                // 负载低，可以增加并发
                if (current < MAX_CONCURRENT_TASKS) {
                    newValue = Math.min(current + 1, MAX_CONCURRENT_TASKS);
                }
                break;
            case BUSY:
                // 负载中等，保持不变
                break;
            case OVERLOADED:
                // 负载高，减少并发
                if (current > MIN_CONCURRENT_TASKS) {
                    newValue = Math.max(current - 1, MIN_CONCURRENT_TASKS);
                }
                break;
        }
        
        if (newValue != current) {
            currentMaxConcurrent.set(newValue);
            logger.info("Concurrent tasks adjusted: {} -> {}", current, newValue);
        }
    }
    
    /**
     * 检查是否允许新任务
     */
    public boolean allowNewTask() {
        return systemState != SystemState.OVERLOADED;
    }
    
    /**
     * 获取当前推荐并发数
     */
    public int getRecommendedConcurrency() {
        return currentMaxConcurrent.get();
    }
    
    /**
     * 获取系统状态
     */
    public SystemState getSystemState() {
        return systemState;
    }
    
    /**
     * 获取系统指标快照
     */
    public SystemMetrics getMetrics() {
        return new SystemMetrics(
            currentCpuLoad,
            currentMemoryUsage,
            availableMemoryMB,
            currentMaxConcurrent.get(),
            systemState
        );
    }
    
    /**
     * 系统指标快照
     */
    public record SystemMetrics(
        double cpuLoad,
        double memoryUsage,
        long availableMemoryMB,
        int maxConcurrentTasks,
        SystemState state
    ) {}
    
    /**
     * 关闭调度器
     */
    public void shutdown() {
        scheduler.shutdown();
    }
}
