package com.videotomd.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * 创建任务请求
 */
public class CreateTaskRequest {

    @NotBlank(message = "视频URL不能为空")
    private String videoUrl;

    public CreateTaskRequest() {
    }

    public CreateTaskRequest(String videoUrl) {
        this.videoUrl = videoUrl;
    }

    public String getVideoUrl() {
        return videoUrl;
    }

    public void setVideoUrl(String videoUrl) {
        this.videoUrl = videoUrl;
    }
}
