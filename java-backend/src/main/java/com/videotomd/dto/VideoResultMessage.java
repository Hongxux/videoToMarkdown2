package com.videotomd.dto;

/**
 * 视频处理结果消息 (从RabbitMQ接收)
 */
public class VideoResultMessage {

    private String taskId;
    private String status;  // COMPLETED / FAILED
    private Float progress;
    private String message;
    private String resultPath;
    private String errorMsg;

    public VideoResultMessage() {
    }

    public VideoResultMessage(String taskId, String status, Float progress, String message, String resultPath, String errorMsg) {
        this.taskId = taskId;
        this.status = status;
        this.progress = progress;
        this.message = message;
        this.resultPath = resultPath;
        this.errorMsg = errorMsg;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Float getProgress() {
        return progress;
    }

    public void setProgress(Float progress) {
        this.progress = progress;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
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
}
