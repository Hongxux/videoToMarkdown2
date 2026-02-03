package com.videotomd.service;

import com.videotomd.entity.UsageLog;
import com.videotomd.repository.UsageLogRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;

/**
 * 限流服务
 */
@Service
public class RateLimitService {

    private final UsageLogRepository usageLogRepository;

    @Value("${app.rate-limit.daily-limit}")
    private int dailyLimit;

    public RateLimitService(UsageLogRepository usageLogRepository) {
        this.usageLogRepository = usageLogRepository;
    }

    /**
     * 检查用户今日是否超过限额
     */
    public boolean checkDailyLimit(Long userId) {
        LocalDate today = LocalDate.now();
        UsageLog usageLog = usageLogRepository.findByUserIdAndActionDate(userId, today)
                .orElse(null);

        if (usageLog == null) {
            return true;  // 今日首次使用
        }

        return usageLog.getActionCount() < dailyLimit;
    }

    /**
     * 获取今日剩余次数
     */
    public int getRemainingTokensToday(Long userId) {
        LocalDate today = LocalDate.now();
        UsageLog usageLog = usageLogRepository.findByUserIdAndActionDate(userId, today)
                .orElse(null);

        if (usageLog == null) {
            return dailyLimit;
        }

        return Math.max(0, dailyLimit - usageLog.getActionCount());
    }

    /**
     * 记录今日使用次数
     */
    @Transactional
    public void incrementUsage(Long userId, String taskId) {
        LocalDate today = LocalDate.now();
        UsageLog usageLog = usageLogRepository.findByUserIdAndActionDate(userId, today)
                .orElse(null);

        if (usageLog == null) {
            // 创建新记录
            usageLog = new UsageLog();
            usageLog.setUserId(userId);
            usageLog.setActionDate(today);
            usageLog.setActionCount(1);
            usageLog.setTaskId(taskId);
        } else {
            // 增加计数
            usageLog.setActionCount(usageLog.getActionCount() + 1);
        }

        usageLogRepository.save(usageLog);
    }
}
