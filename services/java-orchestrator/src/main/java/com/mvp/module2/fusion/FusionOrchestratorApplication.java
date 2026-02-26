package com.mvp.module2.fusion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@SpringBootApplication
@EnableAsync
@EnableScheduling
public class FusionOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(FusionOrchestratorApplication.class, args);
    }

    @Bean
    public ThreadPoolTaskExecutor taskExecutor(
            @Value("${fusion.async.executor.core-pool-size:5}") int corePoolSize,
            @Value("${fusion.async.executor.max-pool-size:10}") int maxPoolSize,
            @Value("${fusion.async.executor.queue-capacity:25}") int queueCapacity,
            @Value("${fusion.async.executor.thread-name-prefix:FusionWorker-}") String threadNamePrefix) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        int normalizedCore = Math.max(1, corePoolSize);
        int normalizedMax = Math.max(normalizedCore, maxPoolSize);
        int normalizedQueue = Math.max(1, queueCapacity);
        executor.setCorePoolSize(normalizedCore);
        executor.setMaxPoolSize(normalizedMax);
        executor.setQueueCapacity(normalizedQueue);
        executor.setThreadNamePrefix(
            threadNamePrefix != null && !threadNamePrefix.isBlank()
                ? threadNamePrefix
                : "FusionWorker-"
        );
        executor.initialize();
        return executor;
    }
}
