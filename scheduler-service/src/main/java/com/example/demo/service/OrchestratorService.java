package com.example.demo.service;

import com.example.demo.config.RabbitMQConfig;
import com.example.demo.dto.DagStatusResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.fasterxml.jackson.core.type.TypeReference;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.StreamSupport;

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
        try {
            // Save the blueprint and the initial PENDING status for all tasks
            redisTemplate.opsForValue().set("dag:" + dagId + ":definition", dagPayload.toString());
            dagPayload.get("tasks").forEach(taskNode -> {
                String taskName = taskNode.get("name").asText();
                redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "PENDING");
            });
            // Immediately evaluate the DAG to run the first tasks
            evaluateDag(dagId);
        } catch (Exception e) {
            LOGGER.error("Failed to process DAG submission", e);
            return null;
        }
        return dagId;
    }

    // The core logic engine for the DAG
    public void evaluateDag(String dagId) {
        try {
            String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
            if (dagJson == null) {
                LOGGER.error("Cannot evaluate DAG. Definition for DAG ID '{}' not found in Redis.", dagId);
                return;
            }
            JsonNode dagPayload = objectMapper.readTree(dagJson);

            dagPayload.get("tasks").forEach(taskNode -> {
                String taskName = taskNode.get("name").asText();
                String currentStatus = redisTemplate.opsForValue().get("dag:" + dagId + ":task:" + taskName + ":status");

                // We only care about tasks that are waiting to be run
                if ("PENDING".equals(currentStatus)) {
                    // Check if all dependencies for this task are met
                    if (areDependenciesMet(dagId, taskNode)) {
                        dispatchTask(dagId, taskNode);
                    }
                }
            });
        } catch (IOException e) {
            LOGGER.error("Failed to read or parse DAG definition for evaluation for DAG ID: {}", dagId, e);
        }
    }

    private boolean areDependenciesMet(String dagId, JsonNode taskNode) {
        if (!taskNode.has("depends_on") || taskNode.get("depends_on").isEmpty()) {
            return true; // No dependencies, so they are always met.
        }
        // This is the key logic: check the status of every dependency in Redis.
        return StreamSupport.stream(taskNode.get("depends_on").spliterator(), false)
                .allMatch(dependencyNode -> {
                    String dependencyName = dependencyNode.asText();
                    String dependencyStatus = redisTemplate.opsForValue().get("dag:" + dagId + ":task:" + dependencyName + ":status");
                    return "SUCCEEDED".equals(dependencyStatus);
                });
    }

    public DagStatusResponse getDagStatus(String dagId) {
        try {
            String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
            if (dagJson == null) {
                LOGGER.warn("Status requested for non-existent DAG ID: {}", dagId);
                return null;
            }

            JsonNode dagPayload = objectMapper.readTree(dagJson);
            String dagName = dagPayload.get("dagName").asText();
            List<DagStatusResponse.TaskStatus> taskStatuses = new ArrayList<>();

            for (JsonNode taskNode : dagPayload.get("tasks")) {
                String taskName = taskNode.get("name").asText();
                String status = redisTemplate.opsForValue().get("dag:" + dagId + ":task:" + taskName + ":status");

                // Fetch logs if they exist
                String logsJson = redisTemplate.opsForValue().get("dag:" + dagId + ":task:" + taskName + ":logs");
                List<String> logs = (logsJson != null)
                        ? objectMapper.readValue(logsJson, new TypeReference<List<String>>(){})
                        : Collections.emptyList();

                List<String> dependsOn = new ArrayList<>();
                if (taskNode.has("depends_on")) {
                    for (JsonNode depNode : taskNode.get("depends_on")) {
                        dependsOn.add(depNode.asText());
                    }
                }
                taskStatuses.add(new DagStatusResponse.TaskStatus(taskName, status, dependsOn, logs));
            }

            return new DagStatusResponse(dagId, dagName, taskStatuses);
        } catch (IOException e) {
            LOGGER.error("Failed to construct DAG status for ID: {}", dagId, e);
            return null;
        }
    }

    private void dispatchTask(String dagId, JsonNode taskNode) {
        String taskName = taskNode.get("name").asText();
        try {
            // Update status to QUEUED before sending to prevent duplicate dispatches
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "QUEUED");

            // Add the dagId to the message payload so the worker can report back
            ObjectNode messagePayload = (ObjectNode) taskNode.deepCopy();
            messagePayload.put("dagId", dagId);

            String message = objectMapper.writeValueAsString(messagePayload);
            rabbitTemplate.convertAndSend(RabbitMQConfig.COMMAND_TASK_QUEUE, message);
            LOGGER.info("Dispatched task '{}' for DAG ID '{}' to the queue.", taskName, dagId);
        } catch (Exception e) {
            LOGGER.error("Failed to dispatch task '{}' for DAG ID '{}'", taskName, dagId, e);
            // If dispatch fails, roll back the status to PENDING so we can try again later
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "PENDING");
        }
    }
}