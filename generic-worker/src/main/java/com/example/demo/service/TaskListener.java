package com.example.demo.service;

import com.example.demo.config.RabbitMQConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import com.github.dockerjava.transport.DockerHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class TaskListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DockerClient dockerClient;
    private final RabbitTemplate rabbitTemplate; // NEW: For sending result messages

    @Autowired // NEW: Spring will inject the RabbitTemplate for us
    public TaskListener(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
        LOGGER.info("Initializing TaskListener...");
        DefaultDockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .build();
        this.dockerClient = DockerClientImpl.getInstance(config, httpClient);
        LOGGER.info("Docker client initialized successfully.");
    }

    @RabbitListener(queues = RabbitMQConfig.COMMAND_TASK_QUEUE)
    public void receiveTask(String taskMessage) {
        LOGGER.info("====== TASK PROCESSING STARTED ======");
        JsonNode taskNode = null;
        String containerId = null;
        int exitCode = -1; // Default to a failure code
        List<String> logs = new ArrayList<>();

        try {
            // Unpack the work order
            taskNode = objectMapper.readTree(taskMessage);
            String taskName = taskNode.get("name").asText();
            String image = taskNode.get("image").asText();
            String[] command = objectMapper.convertValue(taskNode.get("command"), String[].class);

            // Ensure the necessary Docker image is available
            pullImage(image);

            // Create the container
            CreateContainerResponse container = dockerClient.createContainerCmd(image).withCmd(command).exec();
            containerId = container.getId();

            // This nested try-finally ensures the container is always removed
            try {
                // Execute the task
                dockerClient.startContainerCmd(containerId).exec();
                exitCode = dockerClient.waitContainerCmd(containerId).start().awaitStatusCode();
                // Capture the results (the logs)
                logs = captureLogs(containerId);
            } finally {
                dockerClient.removeContainerCmd(containerId).exec();
            }

        } catch (Exception e) {
            LOGGER.error("CRITICAL: An unexpected error occurred during task processing.", e);
            exitCode = -1; // Ensure the exit code reflects failure
            logs.add("Worker-level exception: " + e.getMessage());
        } finally {
            // This block is GUARANTEED to run, ensuring we always report a result back.
            // This is the core of the feedback loop.
            if (taskNode != null) {
                reportResult(taskNode, exitCode, logs);
            }
            LOGGER.info("====== TASK PROCESSING FINISHED ======");
        }
    }

    /**
     * Constructs a result message and sends it to the central results queue.
     */
    private void reportResult(JsonNode taskNode, int exitCode, List<String> logs) {
        try {
            // Create the JSON payload for the result message
            ObjectNode result = objectMapper.createObjectNode();
            result.put("dagId", taskNode.get("dagId").asText());
            result.put("taskName", taskNode.get("name").asText());
            result.put("status", exitCode == 0 ? "SUCCEEDED" : "FAILED");
            result.set("logs", objectMapper.valueToTree(logs));

            String resultMessage = objectMapper.writeValueAsString(result);

            // Send the message to the dedicated results queue
            rabbitTemplate.convertAndSend(RabbitMQConfig.TASK_RESULTS_QUEUE, resultMessage);
            LOGGER.info("Reported final status '{}' for task '{}' to the results queue.", result.get("status").asText(),
                    result.get("taskName").asText());
        } catch (Exception e) {
            LOGGER.error(
                    "CRITICAL: Could not report task result back to the scheduler. This could cause the DAG to stall.",
                    e);
        }
    }

    /**
     * Captures stdout and stderr from a completed container.
     * 
     * @return A List containing all log lines.
     */

    private List<String> captureLogs(String containerId) throws InterruptedException {
        final List<String> logs = new ArrayList<>();
        LogContainerResultCallback loggingCallback = new LogContainerResultCallback() {
            @Override
            public void onNext(Frame item) {
                logs.add(new String(item.getPayload()).trim());
            }
        };
        dockerClient.logContainerCmd(containerId).withStdOut(true).withStdErr(true).exec(loggingCallback)
                .awaitCompletion();
        return logs;
    }

    /**
     * Ensures a Docker image is available locally, pulling it if necessary.
     */
    private void pullImage(String image) throws InterruptedException {
        LOGGER.info("Checking for image '{}' locally...", image);
        try {
            dockerClient.inspectImageCmd(image).exec();
            LOGGER.info("Image '{}' already exists locally.", image);
        } catch (com.github.dockerjava.api.exception.NotFoundException e) {
            LOGGER.warn("Image '{}' not found locally. Pulling from registry...", image);
            dockerClient.pullImageCmd(image).start().awaitCompletion(5, TimeUnit.MINUTES);
            LOGGER.info("Successfully pulled image '{}'.", image);
        }
    }
}