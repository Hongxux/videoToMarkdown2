package com.videotomd.service;

import com.videotomd.dto.VideoTaskMessage;
import com.videotomd.entity.Task;
import com.videotomd.mq.TaskProducer;
import com.videotomd.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.io.*;
import java.net.MalformedURLException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * 任务服务
 */
@Service
public class TaskService {

    private static final Logger log = LoggerFactory.getLogger(TaskService.class);

    private final TaskRepository taskRepository;
    private final TaskProducer taskProducer;
    private final RateLimitService rateLimitService;

    public TaskService(TaskRepository taskRepository,
                      TaskProducer taskProducer,
                      RateLimitService rateLimitService) {
        this.taskRepository = taskRepository;
        this.taskProducer = taskProducer;
        this.rateLimitService = rateLimitService;
    }

    /**
     * 创建任务
     */
    @Transactional
    public Task createTask(String videoUrl, Long userId) {
        // 1. 检查限流
        if (!rateLimitService.checkDailyLimit(userId)) {
            throw new RuntimeException("今日已达使用上限(3次),请明天再试");
        }

        // 2. 创建任务
        String taskId = UUID.randomUUID().toString();
        Task task = new Task();
        task.setTaskId(taskId);
        task.setUserId(userId);
        task.setVideoUrl(videoUrl);
        task.setStatus(Task.TaskStatus.PENDING);
        task.setProgress(0.0f);

        Task savedTask = taskRepository.save(task);
        log.info("任务已创建: taskId={}, userId={}", taskId, userId);

        //3. 发送任务到RabbitMQ
        VideoTaskMessage message = new VideoTaskMessage(
                taskId,
                videoUrl,
                userId,
                LocalDateTime.now().toString()
        );
        taskProducer.sendTask(message);

        // 4. 记录使用次数
        rateLimitService.incrementUsage(userId, taskId);

        return savedTask;
    }

    /**
     * 获取用户的所有任务
     */
    public List<Task> getUserTasks(Long userId) {
        return taskRepository.findByUserIdOrderByCreatedAtDesc(userId);
    }

    /**
     * 获取任务详情
     */
    public Task getTaskDetail(String taskId, Long userId) {
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));

        // 验证任务所有权
        if (!task.getUserId().equals(userId)) {
            throw new RuntimeException("无权访问此任务");
        }

        return task;
    }

    /**
     * 获取今日剩余次数
     */
    public int getRemainingToday(Long userId) {
        return rateLimitService.getRemainingTokensToday(userId);
    }

    /**
     * 下载结果文件 (ZIP格式，包含笔记和素材)
     */
    public ResponseEntity<Resource> downloadResult(String taskId) {
        Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("任务不存在"));

        if (task.getStatus() != Task.TaskStatus.COMPLETED || task.getResultPath() == null) {
            throw new RuntimeException("任务尚未完成或结果不可用");
        }

        try {
            Path resultFilePath = Paths.get(task.getResultPath());
            Path notesDir = resultFilePath.getParent();

            if (notesDir == null || !Files.exists(notesDir)) {
                throw new RuntimeException("结果目录不存在");
            }

            // 创建临时ZIP文件
            Path tempZip = Files.createTempFile("task_result_" + taskId, ".zip");
            zipDirectory(notesDir, tempZip);

            Resource resource = new UrlResource(tempZip.toUri());

            return ResponseEntity.ok()
                    .contentType(MediaType.parseMediaType("application/zip"))
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"notes_" + taskId + ".zip\"")
                    .body(resource);

        } catch (IOException e) {
            log.error("创建ZIP失败", e);
            throw new RuntimeException("文件处理错误: " + e.getMessage());
        }
    }

    /**
     * 递归压缩目录
     */
    private void zipDirectory(Path sourceDir, Path targetZip) throws IOException {
        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(targetZip.toFile()))) {
            Files.walkFileTree(sourceDir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    // 跳过正在被写入的ZIP文件本身（理论上不会在源目录里）
                    String relativePath = sourceDir.relativize(file).toString();
                    zos.putNextEntry(new ZipEntry(relativePath));
                    Files.copy(file, zos);
                    zos.closeEntry();
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }
}
