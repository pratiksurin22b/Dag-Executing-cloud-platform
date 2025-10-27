package com.example.demo.service;

import com.example.demo.config.RabbitMQConfig;
import com.example.demo.dto.DagStatusResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

// Kubernetes Imports (v18.0.0)
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.generic.GenericKubernetesApi;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1JobSpec;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;


import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * The OrchestratorService is the "brain" managing DAG execution via Kubernetes Jobs (v18.0.0 client).
 */
@Service
public class OrchestratorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrchestratorService.class);
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final BatchV1Api batchV1Api;
    private final CoreV1Api coreV1Api;
    private final SharedInformerFactory informerFactory;
    private Indexer<V1Job> jobIndexer;

    @Autowired
    public OrchestratorService(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) throws IOException {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;

        ApiClient client = ClientBuilder.cluster().build();
        client.setReadTimeout(0);
        client.setWriteTimeout(0);
        client.setConnectTimeout(0);
        Configuration.setDefaultApiClient(client);
        this.batchV1Api = new BatchV1Api(client);
        this.coreV1Api = new CoreV1Api(client);
        this.informerFactory = new SharedInformerFactory(client);

        setupJobWatcher();

        LOGGER.info("Kubernetes client and Job watcher initialized successfully.");
    }

    // --- Configure the Kubernetes Job Watcher (Corrected for v18.0.0 API Call Signature) ---
    private void setupJobWatcher() {
        try {
            GenericKubernetesApi<V1Job, V1JobList> jobApi = new GenericKubernetesApi<>(
                    V1Job.class, V1JobList.class, "batch", "v1", "jobs");

            var jobInformer = informerFactory.sharedIndexInformerFor(
                    jobApi,
                    V1Job.class,
                    0L,
                    "helios"
            );

            jobInformer.addEventHandler(new ResourceEventHandler<V1Job>() {
                @Override
                public void onAdd(V1Job job) { /* Ignore */ }

                @Override
                public void onUpdate(V1Job oldJob, V1Job newJob) {
                    String jobName = newJob.getMetadata().getName();
                    LOGGER.debug("Watcher: Job updated - {}", jobName);
                    var status = newJob.getStatus();
                    if (status != null) {
                        boolean succeeded = status.getSucceeded() != null && status.getSucceeded() > 0;
                        boolean failed = status.getFailed() != null && status.getFailed() > 0;
                        if (succeeded || failed) {
                            String processedKey = "job:processed:" + jobName;
                            if (Boolean.TRUE.equals(redisTemplate.opsForValue().setIfAbsent(processedKey, "true"))) {
                                redisTemplate.expire(processedKey, java.time.Duration.ofHours(2));
                                LOGGER.info("Watcher: Job '{}' completed (Succeeded={}, Failed={}). Processing result...", jobName, succeeded, failed);
                                handleJobCompletion(newJob, succeeded);
                            } else {
                                LOGGER.debug("Watcher: Job '{}' completion already processed.", jobName);
                            }
                        }
                    }
                }

                @Override
                public void onDelete(V1Job job, boolean deletedFinalStateUnknown) { /* Ignore */ }
            });
            this.jobIndexer = jobInformer.getIndexer();
        } catch (Exception e) {
            LOGGER.error("Failed to setup Job watcher", e);
        }
    }

    @PostConstruct
    public void startWatchers() {
        LOGGER.info("Starting Kubernetes watchers...");
        informerFactory.startAllRegisteredInformers();
        LOGGER.info("Kubernetes watchers started.");
    }

    @PreDestroy
    public void stopWatchers() {
        LOGGER.info("Stopping Kubernetes watchers...");
        informerFactory.stopAllRegisteredInformers();
        LOGGER.info("Kubernetes watchers stopped.");
    }

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
            LOGGER.error("Failed to process DAG submission for DAG ID: {}", dagId, e);
            return null;
        }
        return dagId;
    }

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

    private boolean areDependenciesMet(String dagId, JsonNode taskNode) {
        if (!taskNode.has("depends_on") || taskNode.get("depends_on").isEmpty()) {
            return true;
        }
        return StreamSupport.stream(taskNode.get("depends_on").spliterator(), false)
                .allMatch(dependencyNode -> "SUCCEEDED".equals(redisTemplate.opsForValue().get("dag:" + dagId + ":task:" + dependencyNode.asText() + ":status")));
    }

    // --- CORRECTED v18.0.0 API CALL PATTERN for createNamespacedJob ---
    private void dispatchTask(String dagId, JsonNode taskNode) {
        String taskName = taskNode.get("name").asText();
        String jobName = generateK8sJobName(dagId, taskName);
        try {
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "K8S_JOB_CREATING");
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":attempts", "1");

            V1Job job = createK8sJobDefinition(jobName, dagId, taskNode);

            LOGGER.info("Submitting Kubernetes Job '{}' for task '{}'...", jobName, taskName);
            // v18.0.0 API call signature
            V1Job createdJob = batchV1Api.createNamespacedJob(
                    "helios",
                    job,
                    "true",
                    null,
                    null,
                    "Strict"
            );

            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "K8S_JOB_SUBMITTED");
            LOGGER.info("Successfully submitted Kubernetes Job '{}'.", createdJob.getMetadata().getName());

        } catch (ApiException e) {
            LOGGER.error("Kubernetes API Error dispatching task '{}' (Job '{}'): Code={}, Body={}",
                    taskName, jobName, e.getCode(), e.getResponseBody(), e);
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "DISPATCH_FAILED");
        } catch (Exception e) {
            LOGGER.error("Failed to create or dispatch Kubernetes Job for task '{}'", taskName, e);
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "DISPATCH_FAILED");
        }
    }

    private void handleJobCompletion(V1Job job, boolean succeeded) {
        V1ObjectMeta metadata = job.getMetadata();
        String jobName = metadata.getName();
        String dagId = metadata.getLabels() != null ? metadata.getLabels().get("helios-dag-id") : null;
        String taskName = metadata.getLabels() != null ? metadata.getLabels().get("helios-task-name") : null;

        if (dagId == null || taskName == null) {
            LOGGER.error("Job '{}' completed but missing required helios labels. Cannot process result.", jobName);
            return;
        }
        LOGGER.info("Processing completion for Job '{}' (Task: '{}', DAG: '{}'). Success={}", jobName, taskName, dagId, succeeded);
        String logs = fetchPodLogsForJob(jobName, "helios");
        processJobResult(dagId, taskName, succeeded, logs);
    }

    // --- CORRECTED v18.0.0 API CALL PATTERN for listNamespacedPod and readNamespacedPodLog ---
    private String fetchPodLogsForJob(String jobName, String namespace) {
        try {
            String labelSelector = "job-name=" + jobName;
            // v18.0.0 API call signature
            V1PodList podList = coreV1Api.listNamespacedPod(
                    namespace,
                    "true",
                    false,
                    null,
                    null,
                    labelSelector,
                    null,
                    null,
                    null,
                    null,
                    false
            );

            if (podList != null && !podList.getItems().isEmpty()) {
                V1Pod pod = podList.getItems().get(0);
                String podName = pod.getMetadata().getName();
                String containerName = pod.getSpec().getContainers().get(0).getName();
                String podPhase = pod.getStatus() != null ? pod.getStatus().getPhase() : "Unknown";

                if ("Succeeded".equals(podPhase) || "Failed".equals(podPhase)) {
                    LOGGER.info("Fetching logs for Pod '{}' (Phase: {}) associated with Job '{}'", podName, podPhase, jobName);
                    // v18.0.0 API call signature
                    String logContent = coreV1Api.readNamespacedPodLog(
                            podName,
                            namespace,
                            containerName,
                            false,
                            false,
                            null,
                            null,
                            false,
                            null,
                            null,
                            false
                    );

                    return logContent != null ? logContent : "";
                } else {
                    return "Pod logs not ready (Phase: " + podPhase + ")";
                }
            } else {
                return "Error: Pod not found.";
            }
        } catch (ApiException e) {
            if (e.getCode() == 404) { return "Error: Pod/Job not found."; }
            LOGGER.error("K8s API Error fetching logs for Job '{}': {}", jobName, e.getResponseBody(), e);
            return "Error fetching logs: API Error " + e.getCode();
        } catch (Exception e) {
            LOGGER.error("Unexpected error fetching logs for Job '{}'", jobName, e);
            return "Error fetching logs: " + e.getMessage();
        }
    }

    private void processJobResult(String dagId, String taskName, boolean success, String logs) {
        try {
            String finalStatus = success ? "SUCCEEDED" : "FAILED";
            String taskStatusKey = String.format("dag:%s:task:%s:status", dagId, taskName);
            String taskLogsKey = String.format("dag:%s:task:%s:logs", dagId, taskName);

            List<String> logList = (logs != null && !logs.startsWith("Error:")) ? List.of(logs.split("\n")) : List.of(logs);
            redisTemplate.opsForValue().set(taskLogsKey, objectMapper.writeValueAsString(logList));

            if (success) {
                String currentStatus = redisTemplate.opsForValue().get(taskStatusKey);
                if (!"SUCCEEDED".equals(currentStatus) && !"FAILED".equals(currentStatus) && !"UPSTREAM_FAILED".equals(currentStatus)) {
                    redisTemplate.opsForValue().set(taskStatusKey, "SUCCEEDED");
                    LOGGER.info("Task '{}' SUCCEEDED. Triggering DAG re-evaluation.", taskName);
                    evaluateDag(dagId);
                } else {
                    LOGGER.warn("Task '{}' already in final state '{}'. Ignoring success event.", taskName, currentStatus);
                }
            } else {
                handleTaskFailure(dagId, taskName);
            }

        } catch (Exception e) {
            LOGGER.error("CRITICAL: Failed to process final job result for task '{}', DAG '{}'.", taskName, dagId, e);
        }
    }

    private void handleTaskFailure(String dagId, String taskName) {
        try {
            String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
            if (dagJson == null) return;
            JsonNode dagPayload = objectMapper.readTree(dagJson);
            JsonNode taskNode = findTaskNode(dagPayload, taskName);
            if (taskNode == null) return;
            int maxRetries = taskNode.path("retries").path("count").asInt(0);
            String attemptKey = String.format("dag:%s:task:%s:attempts", dagId, taskName);
            long attemptsMade = redisTemplate.opsForValue().increment(attemptKey);

            if (attemptsMade <= maxRetries) {
                LOGGER.warn("Task '{}' FAILED on attempt {}. Re-dispatching for retry... (Max retries: {})", taskName, attemptsMade, maxRetries);
                dispatchTask(dagId, taskNode);
            } else {
                LOGGER.error("Task '{}' FAILED on final attempt {}. Initiating failure propagation.", taskName, attemptsMade);
                String taskStatusKey = String.format("dag:%s:task:%s:status", dagId, taskName);
                String currentStatus = redisTemplate.opsForValue().get(taskStatusKey);
                if (!"FAILED".equals(currentStatus) && !"UPSTREAM_FAILED".equals(currentStatus)) {
                    redisTemplate.opsForValue().set(taskStatusKey, "FAILED");
                    propagateFailure(dagId, taskName);
                } else {
                    LOGGER.warn("Task '{}' already in final state '{}'. Ignoring failure event.", taskName, currentStatus);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Unexpected error during failure handling for task '{}', DAG '{}'", taskName, dagId, e);
        }
    }

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
                            String currentChildStatus = redisTemplate.opsForValue().get(childStatusKey);
                            if ("PENDING".equals(currentChildStatus) || "K8S_JOB_CREATING".equals(currentChildStatus) || "K8S_JOB_SUBMITTED".equals(currentChildStatus)) {
                                redisTemplate.opsForValue().set(childStatusKey, "UPSTREAM_FAILED");
                                LOGGER.warn("Propagating failure: Task '{}' marked as UPSTREAM_FAILED.", childTaskName);
                                propagateFailure(dagId, childTaskName);
                            }
                        }
                    });
                }
            });
        } catch (IOException e) {
            LOGGER.error("Failed to read DAG definition during failure propagation for DAG ID: {}", dagId, e);
        }
    }

    public DagStatusResponse getDagStatus(String dagId) {
        try {
            String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
            if (dagJson == null) { return null; }
            JsonNode dagPayload = objectMapper.readTree(dagJson);
            String dagName = dagPayload.get("dagName").asText();
            List<DagStatusResponse.TaskStatus> taskStatuses = new ArrayList<>();

            for (JsonNode taskNode : dagPayload.get("tasks")) {
                String taskName = taskNode.get("name").asText();
                String status = redisTemplate.opsForValue().get(String.format("dag:%s:task:%s:status", dagId, taskName));
                status = (status == null) ? "PENDING" : status;
                String logsJson = redisTemplate.opsForValue().get(String.format("dag:%s:task:%s:logs", dagId, taskName));
                List<String> logs = (logsJson != null) ? objectMapper.readValue(logsJson, new TypeReference<>() {}) : Collections.emptyList();
                List<String> dependsOn = new ArrayList<>();
                if (taskNode.has("depends_on")) {
                    for (JsonNode depNode : taskNode.get("depends_on")) { dependsOn.add(depNode.asText()); }
                }
                taskStatuses.add(new DagStatusResponse.TaskStatus(taskName, status, dependsOn, logs));
            }
            return new DagStatusResponse(dagId, dagName, taskStatuses);
        } catch (IOException e) {
            LOGGER.error("Failed to construct DAG status for ID: {}", dagId, e);
            return null;
        }
    }

    // --- HELPER METHODS ---
    private JsonNode findTaskNode(JsonNode dagPayload, String taskName) {
        for (JsonNode task : dagPayload.get("tasks")) {
            if (task.get("name").asText().equals(taskName)) { return task; }
        }
        return null;
    }

    private String generateK8sJobName(String dagId, String taskName) {
        String cleanDagId = dagId.replaceAll("[^a-z0-9-]", "").toLowerCase();
        String cleanTaskName = taskName.replaceAll("[^a-z0-9-]", "").toLowerCase();
        String base = String.format("helios-%s-%s",
                cleanDagId.substring(Math.max(0, cleanDagId.length() - 8)),
                cleanTaskName);
        base = base.substring(0, Math.min(base.length(), 45)).replaceAll("-$", "");
        return base + "-" + Long.toString(System.nanoTime() % 100000, 36);
    }

    private V1Job createK8sJobDefinition(String jobName, String dagId, JsonNode taskNode) throws IOException {
        String image = taskNode.get("image").asText();
        List<String> command = objectMapper.convertValue(taskNode.get("command"), new TypeReference<>() {});
        String taskName = taskNode.get("name").asText();
        String cleanTaskNameK8s = taskName.replaceAll("[^a-z0-9-]", "").toLowerCase().replaceAll("^-|-$", "");
        cleanTaskNameK8s = cleanTaskNameK8s.substring(0, Math.min(cleanTaskNameK8s.length(), 63));
        String cleanDagIdK8s = dagId.replaceAll("[^a-z0-9-]", "").toLowerCase().replaceAll("^-|-$", "");
        cleanDagIdK8s = cleanDagIdK8s.substring(0, Math.min(cleanDagIdK8s.length(), 63));

        V1Job job = new V1Job();
        job.setApiVersion("batch/v1");
        job.setKind("Job");

        V1ObjectMeta metadata = new V1ObjectMeta();
        metadata.setName(jobName);
        metadata.setNamespace("helios");
        metadata.putLabelsItem("app", "helios-task");
        metadata.putLabelsItem("helios-dag-id", cleanDagIdK8s);
        metadata.putLabelsItem("helios-task-name", cleanTaskNameK8s);
        job.setMetadata(metadata);

        V1JobSpec jobSpec = new V1JobSpec();
        jobSpec.setBackoffLimit(0);

        V1PodTemplateSpec podTemplate = new V1PodTemplateSpec();
        V1ObjectMeta podMeta = new V1ObjectMeta();
        podMeta.putLabelsItem("app", "helios-task");
        podTemplate.setMetadata(podMeta);

        V1PodSpec podSpec = new V1PodSpec();
        podSpec.setRestartPolicy("Never");

        V1Container container = new V1Container();
        container.setName("task-container");
        container.setImage(image);
        container.setCommand(command);
        podSpec.setContainers(List.of(container));

        podTemplate.setSpec(podSpec);
        jobSpec.setTemplate(podTemplate);
        job.setSpec(jobSpec);

        return job;
    }

}
