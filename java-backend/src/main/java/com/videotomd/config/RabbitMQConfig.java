package com.videotomd.config;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * RabbitMQ配置类
 */
@Configuration
public class RabbitMQConfig {

    @Value("${rabbitmq.queue.video-task}")
    private String videoTaskQueueName;

    @Value("${rabbitmq.queue.result}")
    private String resultQueueName;

    /**
     * 视频任务队列
     * - durable: 持久化队列
     * - ttl: 1小时消息过期时间
     */
    @Bean
    public Queue videoTaskQueue() {
        return QueueBuilder.durable(videoTaskQueueName)
                .withArgument("x-message-ttl", 3600000)  // 1小时
                .build();
    }

    /**
     * 结果队列
     */
    @Bean
    public Queue resultQueue() {
        return QueueBuilder.durable(resultQueueName).build();
    }

    /**
     * 消息转换器 (JSON)
     */
    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    /**
     * RabbitTemplate配置
     */
    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter());
        return template;
    }
}
