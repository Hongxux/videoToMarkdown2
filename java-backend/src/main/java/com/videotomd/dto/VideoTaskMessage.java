package com.videotomd.dto;

/**
 * 视频任务消息 (发送到RabbitMQ)
 */
public class VideoTaskMessage {

    private String taskId;
    private String videoUrl;
    private Long userId;
    private String createdAt;

    public VideoTaskMessage() {
    }

    public VideoTaskMessage(String taskId, String videoUrl, Long userId, String createdAt) {
        this.taskId = taskId;
        this.videoUrl = videoUrl;
        this.userId = userId;
        this.createdAt = createdAt;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getVideoUrl() {
        return videoUrl;
    }

    public void setVideoUrl(String videoUrl) {
        this.videoUrl = videoUrl;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }
}
