package com.example.demo.service;

import com.example.demo.config.RabbitMQConfig;
import com.example.demo.dto.DagStatusResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

/**
 * The OrchestratorService is the "brain" of the entire DAG execution platform.
 * It manages the lifecycle of all DAG runs from submission to completion.
 */
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

    /**
     * Processes a new DAG submission from the API controller.
     * This method saves the initial state of the DAG and all its tasks to Redis.
     * @param dagPayload The raw JSON definition of the DAG.
     * @return The unique ID assigned to this new DAG run, or null if processing fails.
     */
    public String processNewDagSubmission(JsonNode dagPayload) {
        String dagId = "dag-" + UUID.randomUUID();
        try {
            redisTemplate.opsForValue().set("dag:" + dagId + ":definition", dagPayload.toString());
            dagPayload.get("tasks").forEach(taskNode -> {
                String taskName = taskNode.get("name").asText();
                redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "PENDING");
            });
            evaluateDag(dagId);
        } catch (Exception e) {
            LOGGER.error("Failed to process DAG submission", e);
            return null;
        }
        return dagId;
    }

    /**
     * The core evaluation engine. This method scans the DAG for any tasks
     * that are ready to be dispatched based on their dependencies.
     * @param dagId The ID of the DAG run to evaluate.
     */
    public void evaluateDag(String dagId) {
        try {
            String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
            if (dagJson == null) { return; }
            JsonNode dagPayload = objectMapper.readTree(dagJson);

            dagPayload.get("tasks").forEach(taskNode -> {
                String taskName = taskNode.get("name").asText();
                String currentStatus = redisTemplate.opsForValue().get("dag:" + dagId + ":task:" + taskName + ":status");
                if ("PENDING".equals(currentStatus) && areDependenciesMet(dagId, taskNode)) {
                    dispatchTask(dagId, taskNode);
                }
            });
        } catch (IOException e) {
            LOGGER.error("Failed to evaluate DAG for ID: {}", dagId, e);
        }
    }

    /**
     * Constructs a complete status report for a given DAG run, to be sent to the UI.
     * This is the method the DagController calls for the GET endpoint.
     * @param dagId The ID of the DAG run to check.
     * @return A DagStatusResponse object containing the state of all tasks, or null if not found.
     */
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
                String logsJson = redisTemplate.opsForValue().get("dag:" + dagId + ":task:" + taskName + ":logs");
                List<String> logs = (logsJson != null) ? objectMapper.readValue(logsJson, new TypeReference<>() {}) : Collections.emptyList();
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

    /**
     * Propagates a failure state downstream through the DAG.
     * When a task fails, this method marks all its children as UPSTREAM_FAILED.
     * @param dagId The ID of the DAG run.
     * @param failedTaskName The name of the task that just failed.
     */
    public void propagateFailure(String dagId, String failedTaskName) {
        try {
            String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
            if (dagJson == null) { return; }
            JsonNode dagPayload = objectMapper.readTree(dagJson);

            dagPayload.get("tasks").forEach(taskNode -> {
                if (taskNode.has("depends_on")) {
                    taskNode.get("depends_on").forEach(depNode -> {
                        if (depNode.asText().equals(failedTaskName)) {
                            String childTaskName = taskNode.get("name").asText();
                            String childStatusKey = String.format("dag:%s:task:%s:status", dagId, childTaskName);
                            redisTemplate.opsForValue().set(childStatusKey, "UPSTREAM_FAILED");
                            LOGGER.warn("Propagating failure: Task '{}' marked as UPSTREAM_FAILED.", childTaskName);
                            propagateFailure(dagId, childTaskName); // Recursive call
                        }
                    });
                }
            });
        } catch (IOException e) {
            LOGGER.error("Failed to read DAG definition during failure propagation for DAG ID: {}", dagId, e);
        }
    }

    private boolean areDependenciesMet(String dagId, JsonNode taskNode) {
        if (!taskNode.has("depends_on") || taskNode.get("depends_on").isEmpty()) {
            return true;
        }
        return StreamSupport.stream(taskNode.get("depends_on").spliterator(), false)
                .allMatch(depNode -> "SUCCEEDED".equals(redisTemplate.opsForValue().get("dag:" + dagId + ":task:" + depNode.asText() + ":status")));
    }

    private void dispatchTask(String dagId, JsonNode taskNode) {
        String taskName = taskNode.get("name").asText();
        try {
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "QUEUED");
            ObjectNode messagePayload = (ObjectNode) taskNode.deepCopy();
            messagePayload.put("dagId", dagId);
            String message = objectMapper.writeValueAsString(messagePayload);
            rabbitTemplate.convertAndSend(RabbitMQConfig.COMMAND_TASK_QUEUE, message);
            LOGGER.info("Dispatched task '{}' for DAG ID '{}'", taskName, dagId);
        } catch (Exception e) {
            LOGGER.error("Failed to dispatch task '{}'", taskName, e);
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "PENDING");
        }
    }
}