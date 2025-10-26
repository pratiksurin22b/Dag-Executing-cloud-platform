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

/**
 * This service is the "ears" of the scheduler. Its primary responsibility is to
 * listen to the task_results_queue for feedback from the workers.
 *
 * When a result is received, this class is responsible for updating the central
 * state in Redis and triggering the next step in the DAG's lifecycle, whether that's
 * running the next tasks or propagating a failure.
 */
@Service
public class ResultListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResultListener.class);
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OrchestratorService orchestratorService;

    /**
     * Constructs the ResultListener with its required dependencies.
     * @param redisTemplate The client for interacting with the Redis state store.
     * @param orchestratorService The core orchestration engine, needed to trigger the next DAG evaluation.
     */
    @Autowired
    public ResultListener(StringRedisTemplate redisTemplate, OrchestratorService orchestratorService) {
        this.redisTemplate = redisTemplate;
        this.orchestratorService = orchestratorService;
    }

    /**
     * The main entry point for processing worker feedback. This method is automatically
     * invoked by Spring AMQP whenever a message arrives on the TASK_RESULTS_QUEUE.
     *
     * @param resultMessage The raw JSON string payload of the result message from a worker.
     */
    @RabbitListener(queues = RabbitMQConfig.TASK_RESULTS_QUEUE)
    public void receiveResult(String resultMessage) {
        try {
            // Log the incoming message for debugging purposes.
            LOGGER.info("Received a new task result from the queue: {}", resultMessage);

            // Parse the raw string into a usable JSON object.
            JsonNode resultNode = objectMapper.readTree(resultMessage);

            // Extract the key identifiers from the result payload.
            String dagId = resultNode.get("dagId").asText();
            String taskName = resultNode.get("taskName").asText();
            String status = resultNode.get("status").asText();

            // --- Step 1: Persist the Logs ---
            // It's important to save the logs regardless of whether the task succeeded or failed.
            if (resultNode.has("logs") && resultNode.get("logs").isArray()) {
                // Convert the logs array back into a JSON string for storage in Redis.
                String logsJson = resultNode.get("logs").toString();
                String taskLogsKey = String.format("dag:%s:task:%s:logs", dagId, taskName);

                redisTemplate.opsForValue().set(taskLogsKey, logsJson);
            }

            // --- Step 2: Update the Task's Final Status ---
            // This is the source of truth for the DAG's state.
            String taskStatusKey = String.format("dag:%s:task:%s:status", dagId, taskName);
            redisTemplate.opsForValue().set(taskStatusKey, status);
            LOGGER.info("Updated status for task '{}' in DAG '{}' to {}", taskName, dagId, status);

            // --- Step 3: The Core Decision-Making Logic ---
            // Based on the final status, decide what to do next.
            if ("SUCCEEDED".equals(status)) {
                // If the task was successful, we must re-evaluate the DAG to see
                // if this completion has unblocked any downstream tasks.
                LOGGER.info("Task succeeded. Triggering DAG re-evaluation for DAG ID: {}", dagId);
                orchestratorService.evaluateDag(dagId);

            } else if ("FAILED".equals(status)) {
                // If the task failed, we must initiate the cascading failure process
                // to mark all downstream tasks as UPSTREAM_FAILED.
                LOGGER.error("Task '{}' in DAG '{}' FAILED. Initiating failure propagation.", taskName, dagId);
                orchestratorService.propagateFailure(dagId, taskName);
            }
            // Note: No action is needed for other statuses, as this listener only handles final results.

        } catch (Exception e) {
            // This is a critical failure. If we can't process a result message,
            // the DAG it belongs to will be permanently stalled.
            LOGGER.error("CRITICAL: Failed to process result message. The DAG may be stuck. Message: '{}'", resultMessage, e);
        }
    }
}