package com.videotomd.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;

/**
 * 任务实体类
 */
@Entity
@Table(name = "tasks")
public class Task {

    @Id
    @Column(name = "task_id", length = 36)
    private String taskId;  // UUID

    @Column(name = "user_id", nullable = false)
    private Long userId;

    @Column(name = "video_url", nullable = false, length = 1000)
    private String videoUrl;

    @Column(name = "video_title", length = 500)
    private String videoTitle;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private TaskStatus status = TaskStatus.PENDING;

    @Column(name = "progress")
    private Float progress = 0.0f;

    @Column(name = "result_path", length = 500)
    private String resultPath;

    @Column(name = "error_msg", columnDefinition = "TEXT")
    private String errorMsg;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    public Task() {
    }

    public Task(String taskId, Long userId, String videoUrl, String videoTitle, TaskStatus status, Float progress, String resultPath, String errorMsg, LocalDateTime createdAt, LocalDateTime updatedAt, LocalDateTime completedAt) {
        this.taskId = taskId;
        this.userId = userId;
        this.videoUrl = videoUrl;
        this.videoTitle = videoTitle;
        this.status = status;
        this.progress = progress;
        this.resultPath = resultPath;
        this.errorMsg = errorMsg;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.completedAt = completedAt;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getVideoUrl() {
        return videoUrl;
    }

    public void setVideoUrl(String videoUrl) {
        this.videoUrl = videoUrl;
    }

    public String getVideoTitle() {
        return videoTitle;
    }

    public void setVideoTitle(String videoTitle) {
        this.videoTitle = videoTitle;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public Float getProgress() {
        return progress;
    }

    public void setProgress(Float progress) {
        this.progress = progress;
    }

    public String getResultPath() {
        return resultPath;
    }

    public void setResultPath(String resultPath) {
        this.resultPath = resultPath;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    public LocalDateTime getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(LocalDateTime completedAt) {
        this.completedAt = completedAt;
    }

    /**
     * 任务状态枚举
     */
    public enum TaskStatus {
        PENDING,      // 等待处理
        PROCESSING,   // 处理中
        COMPLETED,    // 已完成
        FAILED        // 失败
    }
}
