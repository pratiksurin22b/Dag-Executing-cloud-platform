package com.example.demo.config;

import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    public static final String COMMAND_TASK_QUEUE = "command_tasks_queue";
    public static final String TASK_RESULTS_QUEUE = "task_results_queue"; // The new queue

    @Bean
    public Queue commandTaskQueue() {
        return new Queue(COMMAND_TASK_QUEUE, true);
    }

    @Bean // The new bean for the results queue
    public Queue taskResultsQueue() {
        return new Queue(TASK_RESULTS_QUEUE, true);
    }
}