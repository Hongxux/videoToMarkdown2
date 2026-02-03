package com.videotomd.mq;

import com.videotomd.dto.VideoTaskMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 任务生产者 - 发送任务到RabbitMQ
 */
@Component
public class TaskProducer {

    private static final Logger log = LoggerFactory.getLogger(TaskProducer.class);

    private final RabbitTemplate rabbitTemplate;

    @Value("${rabbitmq.queue.video-task}")
    private String videoTaskQueueName;

    public TaskProducer(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    /**
     * 发送任务消息
     */
    public void sendTask(VideoTaskMessage message) {
        try {
            rabbitTemplate.convertAndSend(videoTaskQueueName, message);
            log.info("任务消息已发送到RabbitMQ: taskId={}", message.getTaskId());
        } catch (Exception e) {
            log.error("发送任务消息失败: taskId={}, error={}", message.getTaskId(), e.getMessage());
            throw new RuntimeException("发送任务到队列失败", e);
        }
    }
}
