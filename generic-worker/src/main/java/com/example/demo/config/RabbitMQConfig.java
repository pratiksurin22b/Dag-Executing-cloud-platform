package com.example.demo.config;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    // Define a constant for the queue name to ensure consistency
    public static final String COMMAND_TASK_QUEUE = "command_tasks_queue";

    @Bean
    public Queue commandTaskQueue() {
        // This tells Spring: "When this application starts, please ensure a durable queue
        // with this name exists on the RabbitMQ server."
        //
        // This operation is idempotent:
        // - If the queue does not exist, it will be created.
        // - If the queue already exists, this command does nothing.
        //
        // This is the key to fixing the race condition.
        return new Queue(COMMAND_TASK_QUEUE, true);
    }
}