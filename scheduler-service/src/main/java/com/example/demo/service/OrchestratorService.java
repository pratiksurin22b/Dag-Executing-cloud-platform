package com.example.demo.service;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.demo.config.RabbitMQConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class OrchestratorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrchestratorService.class);
    private final StringRedisTemplate redisTemplate;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public OrchestratorService(StringRedisTemplate redisTemplate, RabbitTemplate rabbitTemplate, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
    }

    public String processNewDagSubmission(JsonNode dagPayload) {
        String dagId = "dag-" + UUID.randomUUID();
        LOGGER.info("Processing new DAG submission. Assigned DAG ID: {}", dagId);

        try {
            // Step 1: Save the entire DAG definition and initial status to Redis
            redisTemplate.opsForValue().set("dag:" + dagId + ":definition", dagPayload.toString());
            redisTemplate.opsForValue().set("dag:" + dagId + ":status", "PENDING");

            // Step 2: Save the initial state for each task
            dagPayload.get("tasks").forEach(taskNode -> {
                String taskName = taskNode.get("name").asText();
                redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "PENDING");
            });

            // Step 3: Find and queue the initial runnable tasks
            findAndQueueRunnableTasks(dagId, dagPayload);

        } catch (Exception e) {
            LOGGER.error("Failed to process DAG submission for DAG ID: {}", dagId, e);
            // In a real app, you would handle this error more gracefully
            return null;
        }

        return dagId;
    }

    private void findAndQueueRunnableTasks(String dagId, JsonNode dagPayload) {
        dagPayload.get("tasks").forEach(taskNode -> {
            String taskName = taskNode.get("name").asText();

            // A task is runnable if it has no dependencies
            if (!taskNode.has("depends_on") || taskNode.get("depends_on").isEmpty()) {
                LOGGER.info("Found initial runnable task '{}' for DAG ID: {}", taskName, dagId);

                // Update status in Redis to QUEUED
                redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "QUEUED");

                // Publish the task to RabbitMQ
                rabbitTemplate.convertAndSend(RabbitMQConfig.COMMAND_TASK_QUEUE, taskNode.toString());
                LOGGER.info("Task '{}' has been sent to the message queue.", taskName);
            }
        });
    }
}