package com.videotomd.mq;

import com.rabbitmq.client.Channel;
import com.videotomd.dto.VideoResultMessage;
import com.videotomd.entity.Task;
import com.videotomd.repository.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;

/**
 * 结果消费者 - 接收Python Worker的处理结果
 */
@Component
public class ResultConsumer {

    private static final Logger log = LoggerFactory.getLogger(ResultConsumer.class);

    private final TaskRepository taskRepository;

    public ResultConsumer(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    /**
     * 消费结果消息
     */
    @RabbitListener(queues = "${rabbitmq.queue.result}")
    public void handleResult(VideoResultMessage message,
                            Channel channel,
                            @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        try {
            log.info("收到结果消息: taskId={}, status={}", message.getTaskId(), message.getStatus());

            Task task = taskRepository.findById(message.getTaskId()).orElse(null);
            if (task == null) {
                log.error("任务不存在: taskId={}", message.getTaskId());
                channel.basicAck(tag, false);
                return;
            }

            // 更新任务状态
            if ("COMPLETED".equals(message.getStatus())) {
                task.setStatus(Task.TaskStatus.COMPLETED);
                task.setProgress(1.0f);
                task.setResultPath(message.getResultPath());
                task.setCompletedAt(LocalDateTime.now());
            } else if ("FAILED".equals(message.getStatus())) {
                task.setStatus(Task.TaskStatus.FAILED);
                task.setErrorMsg(message.getErrorMsg());
                task.setCompletedAt(LocalDateTime.now());
            } else if ("PROCESSING".equals(message.getStatus())) {
                // 进度更新
                task.setStatus(Task.TaskStatus.PROCESSING);
                task.setProgress(message.getProgress());
            }

            taskRepository.save(task);
            log.info("任务状态已更新: taskId={}, status={}", message.getTaskId(), task.getStatus());

            // 手动ACK
            channel.basicAck(tag, false);

        } catch (Exception e) {
            log.error("处理结果消息失败: taskId={}, error={}", message.getTaskId(), e.getMessage());
            // 拒绝消息并重新入队
            channel.basicNack(tag, false, true);
        }
    }
}
