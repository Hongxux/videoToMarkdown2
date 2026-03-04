package com.mvp.module2.fusion.scheduler;

import com.mvp.module2.fusion.service.AdaptiveResourceOrchestrator;
import jakarta.annotation.PostConstruct;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class LoadBasedScheduler {

    private static final Logger logger = LoggerFactory.getLogger(LoadBasedScheduler.class);

    private static final double CPU_HIGH_THRESHOLD = 80.0;
    private static final double CPU_LOW_THRESHOLD = 50.0;
    private static final double MEMORY_HIGH_THRESHOLD = 85.0;
    private static final double JVM_HEAP_HIGH_THRESHOLD = 90.0;
    private static final long MIN_AVAILABLE_MEMORY_MB = 512;

    private static final int MIN_CONCURRENT_TASKS = 1;
    private static final int MAX_CONCURRENT_TASKS = 8;

    @Autowired
    private AdaptiveResourceOrchestrator adaptiveOrchestrator;

    private final AtomicInteger currentMaxConcurrent = new AtomicInteger(4);
    private final OperatingSystemMXBean osMXBean;
    private final ScheduledExecutorService scheduler;

    private volatile double currentCpuLoad = 0.0;
    private volatile double currentMemoryUsage = 0.0;
    private volatile double currentJvmHeapUsage = 0.0;
    private volatile long availableMemoryMB = 0;
    private volatile SystemState systemState = SystemState.NORMAL;

    public enum SystemState {
        NORMAL,
        BUSY,
        OVERLOADED
    }

    public LoadBasedScheduler() {
        this.osMXBean = ManagementFactory.getOperatingSystemMXBean();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @PostConstruct
    public void init() {
        scheduler.scheduleAtFixedRate(this::updateSystemMetrics, 0, 5, TimeUnit.SECONDS);
        logger.info("LoadBasedScheduler initialized");
    }

    private void updateSystemMetrics() {
        try {
            currentCpuLoad = estimateCpuLoad();

            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long maxMemory = runtime.maxMemory();
            long usedHeapMemory = totalMemory - freeMemory;
            currentJvmHeapUsage = maxMemory > 0 ? (double) usedHeapMemory / maxMemory * 100 : 0.0;

            MemorySnapshot systemMemorySnapshot = estimateSystemMemorySnapshot();
            if (systemMemorySnapshot != null) {
                currentMemoryUsage = systemMemorySnapshot.usagePercent();
                availableMemoryMB = systemMemorySnapshot.availableMemoryMB();
            } else {
                currentMemoryUsage = currentJvmHeapUsage;
                availableMemoryMB = Math.max(0L, (maxMemory - usedHeapMemory) / (1024 * 1024));
            }

            updateSystemState();
            adjustConcurrency();

            if (adaptiveOrchestrator != null) {
                adaptiveOrchestrator.adjustConcurrency();
            }
        } catch (Exception exception) {
            logger.warn(
                "Failed to update system metrics (cpu={}%, sysMem={}%, jvmHeap={}%, free={}MB)",
                currentCpuLoad,
                currentMemoryUsage,
                currentJvmHeapUsage,
                availableMemoryMB,
                exception
            );
        }
    }

    private double estimateCpuLoad() {
        try {
            if (osMXBean instanceof com.sun.management.OperatingSystemMXBean sunOsMXBean) {
                double cpuLoad = sunOsMXBean.getCpuLoad();
                if (cpuLoad >= 0) {
                    return cpuLoad * 100;
                }
            }

            double loadAverage = osMXBean.getSystemLoadAverage();
            if (loadAverage >= 0) {
                int cpuCores = Runtime.getRuntime().availableProcessors();
                return Math.min(100.0, (loadAverage / Math.max(cpuCores, 1)) * 100);
            }
        } catch (Exception exception) {
            logger.debug("Estimate cpu load failed: {}", exception.getMessage());
        }
        return 50.0;
    }

    private MemorySnapshot estimateSystemMemorySnapshot() {
        try {
            if (osMXBean instanceof com.sun.management.OperatingSystemMXBean sunOsMXBean) {
                long totalPhysicalMemory = sunOsMXBean.getTotalPhysicalMemorySize();
                long freePhysicalMemory = sunOsMXBean.getFreePhysicalMemorySize();
                if (totalPhysicalMemory > 0 && freePhysicalMemory >= 0) {
                    long usedPhysicalMemory = totalPhysicalMemory - freePhysicalMemory;
                    double memoryUsage = (double) usedPhysicalMemory / totalPhysicalMemory * 100;
                    long freeMemoryInMB = freePhysicalMemory / (1024 * 1024);
                    return new MemorySnapshot(memoryUsage, freeMemoryInMB);
                }
            }
        } catch (Exception exception) {
            logger.debug("Estimate system memory failed: {}", exception.getMessage());
        }
        return null;
    }

    private void updateSystemState() {
        SystemState previousState = systemState;

        if (currentCpuLoad > CPU_HIGH_THRESHOLD
            || currentMemoryUsage > MEMORY_HIGH_THRESHOLD
            || currentJvmHeapUsage > JVM_HEAP_HIGH_THRESHOLD
            || availableMemoryMB < MIN_AVAILABLE_MEMORY_MB) {
            systemState = SystemState.OVERLOADED;
        } else if (currentCpuLoad > CPU_LOW_THRESHOLD) {
            systemState = SystemState.BUSY;
        } else {
            systemState = SystemState.NORMAL;
        }

        if (systemState != previousState) {
            logger.info(
                "System state changed: {} -> {} (CPU={}%, SysMem={}%, JvmHeap={}%, Free={}MB)",
                previousState,
                systemState,
                currentCpuLoad,
                currentMemoryUsage,
                currentJvmHeapUsage,
                availableMemoryMB
            );
        }
    }

    private void adjustConcurrency() {
        int current = currentMaxConcurrent.get();
        int newValue = current;

        switch (systemState) {
            case NORMAL:
                if (current < MAX_CONCURRENT_TASKS) {
                    newValue = Math.min(current + 1, MAX_CONCURRENT_TASKS);
                }
                break;
            case BUSY:
                break;
            case OVERLOADED:
                if (current > MIN_CONCURRENT_TASKS) {
                    newValue = Math.max(current - 1, MIN_CONCURRENT_TASKS);
                }
                break;
            default:
                break;
        }

        if (newValue != current) {
            currentMaxConcurrent.set(newValue);
            logger.info("Concurrent tasks adjusted: {} -> {}", current, newValue);
        }
    }

    public boolean allowNewTask() {
        return systemState != SystemState.OVERLOADED;
    }

    public int getRecommendedConcurrency() {
        return currentMaxConcurrent.get();
    }

    public SystemState getSystemState() {
        return systemState;
    }

    public SystemMetrics getMetrics() {
        return new SystemMetrics(
            currentCpuLoad,
            currentMemoryUsage,
            currentJvmHeapUsage,
            availableMemoryMB,
            currentMaxConcurrent.get(),
            systemState
        );
    }

    public record SystemMetrics(
        double cpuLoad,
        double memoryUsage,
        double jvmHeapUsage,
        long availableMemoryMB,
        int maxConcurrentTasks,
        SystemState state
    ) {}

    private record MemorySnapshot(double usagePercent, long availableMemoryMB) {}

    public void shutdown() {
        scheduler.shutdown();
    }
}
