package com.videotomd.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * 使用记录实体类 (用于限流)
 */
@Entity
@Table(name = "usage_logs", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"user_id", "action_date"})
})
public class UsageLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "log_id")
    private Long logId;

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "task_id", length = 36)
    private String taskId;

    @Column(name = "action_date", nullable = false)
    private LocalDate actionDate;

    @Column(name = "action_count", nullable = false)
    private Integer actionCount = 1;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    public UsageLog() {
    }

    public UsageLog(Long logId, Long userId, String taskId, LocalDate actionDate, Integer actionCount, LocalDateTime createdAt) {
        this.logId = logId;
        this.userId = userId;
        this.taskId = taskId;
        this.actionDate = actionDate;
        this.actionCount = actionCount;
        this.createdAt = createdAt;
    }

    public Long getLogId() {
        return logId;
    }

    public void setLogId(Long logId) {
        this.logId = logId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public LocalDate getActionDate() {
        return actionDate;
    }

    public void setActionDate(LocalDate actionDate) {
        this.actionDate = actionDate;
    }

    public Integer getActionCount() {
        return actionCount;
    }

    public void setActionCount(Integer actionCount) {
        this.actionCount = actionCount;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }
}
