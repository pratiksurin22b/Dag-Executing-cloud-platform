package com.example.demo.config;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String COMMAND_TASK_QUEUE = "command_tasks_queue";

    @Bean
    public Queue commandTaskQueue() {
        // This creates a durable queue named "command_tasks_queue"
        return new Queue(COMMAND_TASK_QUEUE, true);
    }
}