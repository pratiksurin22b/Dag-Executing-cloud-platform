package com.example.demo.config;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    // A constant for the queue the worker LISTENS to
    public static final String COMMAND_TASK_QUEUE = "command_tasks_queue";

    // A constant for the queue the worker SENDS results to
    public static final String TASK_RESULTS_QUEUE = "task_results_queue";

    @Bean
    public Queue commandTaskQueue() {
        // Ensures the queue for receiving tasks exists
        return new Queue(COMMAND_TASK_QUEUE, true);
    }

    @Bean
    public Queue taskResultsQueue() {
        // Ensures the queue for sending results exists
        return new Queue(TASK_RESULTS_QUEUE, true);
    }
}