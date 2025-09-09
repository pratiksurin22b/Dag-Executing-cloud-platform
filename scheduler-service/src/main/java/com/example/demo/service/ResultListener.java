package com.example.demo.service;

import com.example.demo.config.RabbitMQConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
public class ResultListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultListener.class);
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OrchestratorService orchestratorService;

    @Autowired
    public ResultListener(StringRedisTemplate redisTemplate, OrchestratorService orchestratorService) {
        this.redisTemplate = redisTemplate;
        this.orchestratorService = orchestratorService;
    }

    @RabbitListener(queues = RabbitMQConfig.TASK_RESULTS_QUEUE)
    public void receiveResult(String resultMessage) {
        try {
            LOGGER.info("Received a new task result from the queue: {}", resultMessage);
            JsonNode resultNode = objectMapper.readTree(resultMessage);
            String dagId = resultNode.get("dagId").asText();
            String taskName = resultNode.get("taskName").asText();
            String status = resultNode.get("status").asText();

            // <-- NEW LOGIC STARTS HERE -->

            // Step 1: Extract and save the logs from the result message.
            if (resultNode.has("logs") && resultNode.get("logs").isArray()) {
                // Convert the logs array to a JSON string for storage in Redis.
                String logsJson = resultNode.get("logs").toString();
                String taskLogsKey = String.format("dag:%s:task:%s:logs", dagId, taskName);

                redisTemplate.opsForValue().set(taskLogsKey, logsJson);
                LOGGER.info("Saved {} log lines for task '{}' in DAG '{}'", resultNode.get("logs").size(), taskName, dagId);
            }

            // <-- NEW LOGIC ENDS HERE -->

            // Step 2: Update the task's final status in Redis.
            String taskStatusKey = String.format("dag:%s:task:%s:status", dagId, taskName);
            redisTemplate.opsForValue().set(taskStatusKey, status);
            LOGGER.info("Updated status for task '{}' in DAG '{}' to {}", taskName, dagId, status);

            // Step 3: If the task succeeded, re-evaluate the DAG to run the next tasks.
            if ("SUCCEEDED".equals(status)) {
                LOGGER.info("Task succeeded. Triggering DAG re-evaluation for DAG ID: {}", dagId);
                orchestratorService.evaluateDag(dagId);
            } else {
                LOGGER.error("Task '{}' in DAG '{}' FAILED. Halting this branch of the DAG.", taskName, dagId);
                // Future: This is where retry logic would be triggered.
            }

        } catch (Exception e) {
            LOGGER.error("CRITICAL: Failed to process result message.", e);
        }
    }
}