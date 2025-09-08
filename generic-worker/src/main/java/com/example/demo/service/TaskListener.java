package com.example.demo.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class TaskListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DockerClient dockerClient;

    public TaskListener() {
        LOGGER.info("Initializing TaskListener...");
        DefaultDockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        DockerHttpClient httpClient = new ApacheDockerHttpClient.Builder()
                .dockerHost(config.getDockerHost())
                .sslConfig(config.getSSLConfig())
                .build();
        this.dockerClient = DockerClientImpl.getInstance(config, httpClient);
        LOGGER.info("Docker client initialized successfully using Apache HttpClient5 transport.");
    }

    @RabbitListener(queues = "command_tasks_queue")
    public void receiveTask(String taskMessage) {
        LOGGER.info("====== TASK PROCESSING STARTED ======");
        LOGGER.debug("Received raw message from queue: {}", taskMessage);

        String containerId = null;
        try {
            // Step 1: Parse the incoming message
            JsonNode taskNode = parseTaskMessage(taskMessage);
            String taskName = taskNode.get("name").asText();
            String image = taskNode.get("image").asText();
            String[] command = objectMapper.convertValue(taskNode.get("command"), String[].class);
            LOGGER.info("Successfully parsed task '{}'. Image: [{}], Command: [{}]", taskName, image, String.join(" ", command));

            // NEW STEP: Pull the Docker image to ensure it exists locally
            pullImage(image);

            // Step 2: Create the Docker container
            LOGGER.info("Task '{}': Attempting to create container...", taskName);
            CreateContainerResponse container = dockerClient.createContainerCmd(image)
                    .withCmd(command)
                    .exec();
            containerId = container.getId();
            LOGGER.info("Task '{}': Container created successfully with ID: {}", taskName, containerId);

            // This nested try-finally ensures the container is always removed
            try {
                // Step 3: Start the container
                LOGGER.info("Task '{}': Starting container...", taskName);
                dockerClient.startContainerCmd(containerId).exec();
                LOGGER.info("Task '{}': Container started.", taskName);

                // Step 4: Wait for the container to complete
                LOGGER.info("Task '{}': Waiting for container to finish execution...", taskName);
                int exitCode = dockerClient.waitContainerCmd(containerId).start().awaitStatusCode();
                LOGGER.info("Task '{}': Container finished with Exit Code: {}", taskName, exitCode);

                // Step 5: Capture the logs
                logContainerOutput(taskName, containerId);

            } finally {
                // Step 6: Guaranteed Cleanup
                LOGGER.info("Task '{}': Initiating cleanup. Removing container...", taskName);
                dockerClient.removeContainerCmd(containerId).exec();
                LOGGER.info("Task '{}': Container removed successfully.", taskName);
            }

        } catch (JsonProcessingException e) {
            LOGGER.error("CRITICAL: Failed to parse JSON message from queue. The message is malformed. Message: '{}'", taskMessage, e);
        } catch (Exception e) {
            LOGGER.error("CRITICAL: An unexpected error occurred during task processing. Container ID (if created): {}", containerId, e);
            // Emergency cleanup
            if (containerId != null) {
                try {
                    LOGGER.warn("Attempting emergency cleanup of container {}", containerId);
                    dockerClient.removeContainerCmd(containerId).withForce(true).exec();
                    LOGGER.warn("Emergency cleanup successful for container {}", containerId);
                } catch (Exception cleanupException) {
                    LOGGER.error("CRITICAL: Failed to perform emergency cleanup for container {}. Manual intervention may be required.", containerId, cleanupException);
                }
            }
        } finally {
            LOGGER.info("====== TASK PROCESSING FINISHED ======");
        }
    }

    private JsonNode parseTaskMessage(String message) throws JsonProcessingException {
        if (message == null || message.trim().isEmpty()) {
            throw new IllegalArgumentException("Received an empty or null task message from the queue.");
        }
        return objectMapper.readTree(message);
    }

    private void pullImage(String image) throws InterruptedException {
        LOGGER.info("Checking for image '{}' locally...", image);
        try {
            dockerClient.inspectImageCmd(image).exec();
            LOGGER.info("Image '{}' already exists locally. No pull needed.", image);
        } catch (com.github.dockerjava.api.exception.NotFoundException e) {
            LOGGER.warn("Image '{}' not found locally. Pulling from registry...", image);
            // This will pull the image from Docker Hub. It can take some time.
            dockerClient.pullImageCmd(image).start().awaitCompletion(5, TimeUnit.MINUTES);
            LOGGER.info("Successfully pulled image '{}'.", image);
        }
    }

    private void logContainerOutput(String taskName, String containerId) throws InterruptedException {
        LOGGER.info("Task '{}': Fetching logs from container...", taskName);
        final LogContainerResultCallback loggingCallback = new LogContainerResultCallback() {
            @Override
            public void onNext(Frame item) {
                LOGGER.info("[Task: {}] [Container Log] > {}", taskName, new String(item.getPayload()).trim());
            }
        };

        dockerClient.logContainerCmd(containerId)
                .withStdOut(true)
                .withStdErr(true)
                .exec(loggingCallback)
                .awaitCompletion();
        LOGGER.info("Task '{}': Finished fetching logs.", taskName);
    }
}