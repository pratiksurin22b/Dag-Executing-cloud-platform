package com.example.demo.service;

// All existing imports from your v18 file...
import com.example.demo.config.RabbitMQConfig;
import com.example.demo.dto.DagStatusResponse;
import com.example.demo.dto.SystemStatusResponse; // ensure import
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays; // NEW Import
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map; // NEW Import
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors; // NEW Import
import java.util.stream.StreamSupport;

// Kubernetes Imports (v17.x)
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*; // Keep model imports
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Streams;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.util.CallGeneratorParams;
import okhttp3.Call;
// import io.kubernetes.client.util.generic.GenericKubernetesApi; // This was from v19+, v18 uses direct call

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
    private final AppsV1Api appsV1Api;
    private final SharedInformerFactory informerFactory;
    private Indexer<V1Job> jobIndexer;

    // Map of our core components and their app labels in Kubernetes
    private static final Map<String, String> CORE_SERVICES = Map.of(
            "Scheduler", "scheduler-service",
            "Frontend", "frontend",
            "Redis", "redis", // fixed label to match manifest
            "RabbitMQ", "rabbitmq"
    );


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
        this.appsV1Api = new AppsV1Api(client);
        this.informerFactory = new SharedInformerFactory(client);

        setupJobWatcher();

        LOGGER.info("Kubernetes client and Job watcher initialized successfully.");
    }

    // --- Configure the Kubernetes Job Watcher (Corrected for v18.0.0 API Call Signature) ---
    private void setupJobWatcher() {
        var jobInformer = informerFactory.sharedIndexInformerFor(
                (CallGeneratorParams params) -> {
                    try {
                        return batchV1Api.listNamespacedJobCall(
                                "helios", // namespace
                                null, // pretty
                                null, // allowWatchBookmarks
                                null, // _continue
                                null, // fieldSelector
                                "app=helios-task", // labelSelector to only watch our jobs
                                null, // limit
                                params.resourceVersion, // resourceVersion from params
                                null, // resourceVersionMatch
                                params.timeoutSeconds, // timeoutSeconds from params
                                params.watch, // watch from params
                                null // _callback
                        );
                    } catch (ApiException e) {
                        LOGGER.error("Watcher: Failed to create list call for Jobs: {}", e.getResponseBody(), e);
                        throw new RuntimeException(e);
                    }
                },
                V1Job.class, V1JobList.class
        );

        // --- Event Handler logic (Unchanged) ---
        jobInformer.addEventHandler(new ResourceEventHandler<V1Job>() {
            @Override public void onAdd(V1Job job) { /* Ignore */ }
            @Override
            public void onUpdate(V1Job oldJob, V1Job newJob) {
                // ... (existing watcher logic as before) ...
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
            @Override public void onDelete(V1Job job, boolean deletedFinalStateUnknown) { /* Ignore */ }
        });
        this.jobIndexer = jobInformer.getIndexer();
    }

    // --- Start/Stop watchers (Unchanged) ---
    @PostConstruct
    public void startWatchers() {
        LOGGER.info("Starting Kubernetes watchers...");
        informerFactory.startAllRegisteredInformers();
        LOGGER.info("Kubernetes watchers started.");
    }
    @PreDestroy
    public void stopWatchers() {
        LOGGER.info("Stopping Kubernetes watchers...");
        informerFactory.stopAllRegisteredInformers(true); // Graceful shutdown
        LOGGER.info("Kubernetes watchers stopped.");
    }

    // --- NEW METHOD: Get System Health Status ---
    public SystemStatusResponse getSystemStatus() throws ApiException {
        List<SystemStatusResponse.ServiceStatus> serviceStatuses = new ArrayList<>();
        String namespace = "helios";

        V1PodList allPods = coreV1Api.listNamespacedPod(
                namespace, null, null, null, null, null, null, null, null, null, null
        );
        Map<String, List<V1Pod>> podsByApp = allPods.getItems().stream()
                .filter(pod -> pod.getMetadata() != null && pod.getMetadata().getLabels() != null)
                .collect(Collectors.groupingBy(pod -> pod.getMetadata().getLabels().get("app")));

        for (Map.Entry<String, String> serviceEntry : CORE_SERVICES.entrySet()) {
            String serviceName = serviceEntry.getKey();
            String appLabel = serviceEntry.getValue();

            List<V1Pod> matchingPods = podsByApp.getOrDefault(appLabel, Collections.emptyList());

            int desiredReplicas = 0;
            boolean hasDeploymentOrSts = false;
            try {
                String labelSelector = "app=" + appLabel;
                V1DeploymentList dpls = appsV1Api.listNamespacedDeployment(
                        namespace, null, null, null, null, labelSelector, null, null, null, null, null
                );
                if (dpls != null && dpls.getItems() != null && !dpls.getItems().isEmpty()) {
                    hasDeploymentOrSts = true;
                    desiredReplicas += dpls.getItems().stream()
                            .map(d -> d.getSpec() != null && d.getSpec().getReplicas() != null ? d.getSpec().getReplicas() : 0)
                            .reduce(0, Integer::sum);
                }
                V1StatefulSetList stss = appsV1Api.listNamespacedStatefulSet(
                        namespace, null, null, null, null, labelSelector, null, null, null, null, null
                );
                if (stss != null && stss.getItems() != null && !stss.getItems().isEmpty()) {
                    hasDeploymentOrSts = true;
                    desiredReplicas += stss.getItems().stream()
                            .map(s -> s.getSpec() != null && s.getSpec().getReplicas() != null ? s.getSpec().getReplicas() : 0)
                            .reduce(0, Integer::sum);
                }
            } catch (ApiException e) {
                desiredReplicas = 0;
            }
            // If no controller found, infer desired from pods presence (fallback 1 if any pod exists)
            if (!hasDeploymentOrSts) {
                desiredReplicas = matchingPods.isEmpty() ? 0 : 1;
            }

            int readyReplicas = 0;
            int notReadyReplicas = 0;
            int pendingReplicas = 0;
            int crashLoopingReplicas = 0;
            int imagePullBackOffReplicas = 0;
            int runningPhaseReplicas = 0;
            int unresponsiveReplicas = 0; // Running but not Ready and no explicit waiting reason

            List<SystemStatusResponse.PodBrief> podBriefs = new ArrayList<>();
            for (V1Pod pod : matchingPods) {
                String phase = pod.getStatus() != null ? pod.getStatus().getPhase() : "Unknown";
                boolean allContainersReady = true;
                int totalRestarts = 0;
                String reason = null;
                Set<String> waitingReasons = new HashSet<>();
                List<String> trueConditions = new ArrayList<>();

                if (pod.getStatus() != null && pod.getStatus().getConditions() != null) {
                    for (V1PodCondition c : pod.getStatus().getConditions()) {
                        if ("True".equalsIgnoreCase(c.getStatus())) {
                            if (c.getType() != null) trueConditions.add(c.getType());
                        }
                    }
                }

                if (pod.getStatus() != null && pod.getStatus().getContainerStatuses() != null) {
                    for (V1ContainerStatus cStatus : pod.getStatus().getContainerStatuses()) {
                        totalRestarts += cStatus.getRestartCount() != null ? cStatus.getRestartCount() : 0;
                        boolean ready = Boolean.TRUE.equals(cStatus.getReady());
                        if (!ready) allContainersReady = false;
                        if (cStatus.getState() != null) {
                            V1ContainerStateWaiting waiting = cStatus.getState().getWaiting();
                            if (waiting != null && waiting.getReason() != null) {
                                reason = waiting.getReason();
                                waitingReasons.add(waiting.getReason());
                                if ("CrashLoopBackOff".equals(waiting.getReason())) crashLoopingReplicas++;
                                if (waiting.getReason().contains("ImagePullBackOff") || waiting.getReason().contains("ErrImagePull")) imagePullBackOffReplicas++;
                            }
                        }
                    }
                } else {
                    allContainersReady = false;
                }

                if ("Pending".equals(phase)) pendingReplicas++;
                if ("Running".equals(phase)) runningPhaseReplicas++;
                if (allContainersReady && "Running".equals(phase)) {
                    readyReplicas++;
                } else if ("Running".equals(phase)) {
                    notReadyReplicas++;
                    if (waitingReasons.isEmpty()) {
                        unresponsiveReplicas++;
                    }
                }

                podBriefs.add(new SystemStatusResponse.PodBrief(
                        pod.getMetadata() != null ? pod.getMetadata().getName() : "unknown",
                        phase,
                        allContainersReady && "Running".equals(phase),
                        totalRestarts,
                        reason,
                        trueConditions,
                        new ArrayList<>(waitingReasons)
                ));
            }

            // Derive high-level status
            String status;
            if (hasDeploymentOrSts && desiredReplicas == 0) {
                status = "ScaledDown";
            } else if (desiredReplicas == 0) {
                status = runningPhaseReplicas > 0 ? "Healthy" : "Stopped";
            } else if (runningPhaseReplicas == 0 && pendingReplicas > 0) {
                status = "Starting";
            } else if (readyReplicas == desiredReplicas && desiredReplicas > 0) {
                status = "Healthy";
            } else if (crashLoopingReplicas > 0) {
                status = "CrashLoop";
            } else if (imagePullBackOffReplicas > 0) {
                status = "ImagePullError";
            } else if (unresponsiveReplicas > 0) {
                status = "Unresponsive";
            } else if (readyReplicas == 0 && runningPhaseReplicas > 0) {
                status = "NotReady";
            } else if (readyReplicas < desiredReplicas && readyReplicas > 0) {
                status = "Degraded";
            } else if (runningPhaseReplicas == 0 && desiredReplicas > 0) {
                status = "Stopped";
            } else {
                status = "Unknown";
            }

            String firstRunningPodName = matchingPods.stream()
                    .filter(p -> "Running".equals(p.getStatus() != null ? p.getStatus().getPhase() : null))
                    .map(p -> p.getMetadata() != null ? p.getMetadata().getName() : "N/A")
                    .findFirst().orElse("N/A");

            serviceStatuses.add(new SystemStatusResponse.ServiceStatus(
                    serviceName,
                    status,
                    runningPhaseReplicas,
                    desiredReplicas,
                    firstRunningPodName,
                    readyReplicas,
                    notReadyReplicas,
                    pendingReplicas,
                    crashLoopingReplicas,
                    imagePullBackOffReplicas,
                    podBriefs
            ));
        }
        return new SystemStatusResponse(serviceStatuses);
    }
    // --- End of new method ---


    // --- processNewDagSubmission (Unchanged) ---
    public String processNewDagSubmission(JsonNode dagPayload) {
        String dagId = "dag-" + UUID.randomUUID();
        try {
            redisTemplate.opsForValue().set("dag:" + dagId + ":definition", dagPayload.toString());
            dagPayload.get("tasks").forEach(taskNode -> {
                String taskName = taskNode.get("name").asText();
                redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "PENDING");
                redisTemplate.delete(String.format("dag:%s:task:%s:attempts", dagId, taskName));
                redisTemplate.delete(String.format("dag:%s:task:%s:logs", dagId, taskName));
            });
            evaluateDag(dagId);
        } catch (Exception e) {
            LOGGER.error("Failed to process DAG submission for DAG ID: {}", dagId, e);
            return null;
        }
        return dagId;
    }

    // --- evaluateDag (Unchanged) ---
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

    // --- areDependenciesMet (Unchanged) ---
    private boolean areDependenciesMet(String dagId, JsonNode taskNode) {
        if (!taskNode.has("depends_on") || taskNode.get("depends_on").isEmpty()) {
            return true;
        }
        return StreamSupport.stream(taskNode.get("depends_on").spliterator(), false)
                .allMatch(dependencyNode -> "SUCCEEDED".equals(redisTemplate.opsForValue().get("dag:" + dagId + ":task:" + dependencyNode.asText() + ":status")));
    }

    // --- dispatchTask (v18 pattern - Unchanged) ---
    private void dispatchTask(String dagId, JsonNode taskNode) {
        String taskName = taskNode.get("name").asText();
        String jobName = generateK8sJobName(dagId, taskName);
        try {
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "K8S_JOB_CREATING");
            String attemptKey = String.format("dag:%s:task:%s:attempts", dagId, taskName);
            String currentAttemptStr = redisTemplate.opsForValue().get(attemptKey);
            long nextAttempt = (currentAttemptStr == null) ? 1 : (Long.parseLong(currentAttemptStr) + 1);
            redisTemplate.opsForValue().set(attemptKey, String.valueOf(nextAttempt));

            V1Job job = createK8sJobDefinition(jobName, dagId, taskNode);
            LOGGER.info("Submitting Kubernetes Job '{}' for task '{}' (Attempt {})...", jobName, taskName, nextAttempt);
            V1Job createdJob = batchV1Api.createNamespacedJob(
                    "helios", job, null, null, null, null
            );
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "K8S_JOB_SUBMITTED");
            LOGGER.info("Successfully submitted Kubernetes Job '{}'.", createdJob.getMetadata().getName());

        } catch (ApiException e) {
            LOGGER.error("Kubernetes API Error dispatching task '{}': {}", taskName, e.getResponseBody(), e);
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "DISPATCH_FAILED");
            handleTaskFailure(dagId, taskName);
        } catch (Exception e) {
            LOGGER.error("Failed to create/dispatch K8s Job for task '{}'", taskName, e);
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "DISPATCH_FAILED");
            handleTaskFailure(dagId, taskName);
        }
    }

    // --- handleJobCompletion (Unchanged) ---
    private void handleJobCompletion(V1Job job, boolean succeeded) {
        V1ObjectMeta metadata = job.getMetadata();
        String jobName = metadata.getName();
        String dagId = metadata.getLabels() != null ? metadata.getLabels().get("helios-dag-id") : null;
        String taskName = metadata.getLabels() != null ? metadata.getLabels().get("helios-task-name") : null;

        if (dagId == null || taskName == null) {
            LOGGER.error("Job '{}' completed but missing required helios labels.", jobName);
            return;
        }
        LOGGER.info("Processing completion for Job '{}' (Task: '{}', DAG: '{}'). Success={}", jobName, taskName, dagId, succeeded);
        String logs = fetchPodLogsForJob(jobName, "helios");
        processJobResult(dagId, taskName, succeeded, logs);
    }

    // --- fetchPodLogsForJob (v18 pattern - Unchanged) ---
    private String fetchPodLogsForJob(String jobName, String namespace) {
        try {
            String labelSelector = "job-name=" + jobName;
            V1PodList podList = coreV1Api.listNamespacedPod(
                    namespace, null, null, null, null, labelSelector, null, null, null, null, null
            );

            if (podList != null && !podList.getItems().isEmpty()) {
                V1Pod pod = podList.getItems().get(0);
                String podName = pod.getMetadata().getName();
                String containerName = (pod.getSpec() != null && !pod.getSpec().getContainers().isEmpty())
                        ? pod.getSpec().getContainers().get(0).getName() : null;
                if (containerName == null) {
                    return "Error: Container name not found.";
                }
                String podPhase = pod.getStatus() != null ? pod.getStatus().getPhase() : "Unknown";

                if ("Succeeded".equals(podPhase) || "Failed".equals(podPhase)) {
                    String logContent = coreV1Api.readNamespacedPodLog(
                            podName,                          // name
                            namespace,                        // namespace
                            containerName,                    // container
                            Boolean.FALSE,                    // follow
                            Boolean.FALSE,                    // insecureSkipTLSVerifyBackend or pretty (per v18)
                            (Integer) null,                   // limitBytes
                            (String) null,                    // sinceTime
                            Boolean.FALSE,                    // timestamps
                            (Integer) null,                   // sinceSeconds
                            (Integer) null,                   // tailLines
                            Boolean.FALSE                     // limitBytes? or some boolean flag (per v18)
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


    // --- processJobResult (Refined logic - Unchanged) ---
    private void processJobResult(String dagId, String taskName, boolean success, String logs) {
        try {
            String taskStatusKey = String.format("dag:%s:task:%s:status", dagId, taskName);
            String taskLogsKey = String.format("dag:%s:task:%s:logs", dagId, taskName);
            List<String> logList = (logs != null && !logs.startsWith("Error:")) ? List.of(logs.split("\n")) : List.of(logs);
            redisTemplate.opsForValue().set(taskLogsKey, objectMapper.writeValueAsString(logList));
            String currentStatus = redisTemplate.opsForValue().get(taskStatusKey);
            if ("SUCCEEDED".equals(currentStatus) || "FAILED".equals(currentStatus) || "UPSTREAM_FAILED".equals(currentStatus)) {
                LOGGER.warn("Task '{}' already in final state '{}'. Ignoring job completion event.", taskName, currentStatus);
                return;
            }
            if (success) {
                redisTemplate.opsForValue().set(taskStatusKey, "SUCCEEDED");
                LOGGER.info("Task '{}' SUCCEEDED. Triggering DAG re-evaluation.", taskName);
                evaluateDag(dagId);
            } else {
                handleTaskFailure(dagId, taskName);
            }
        } catch (Exception e) {
            LOGGER.error("CRITICAL: Failed to process final job result for task '{}', DAG '{}'.", taskName, dagId, e);
        }
    }


    // --- handleTaskFailure (Refined logic - Unchanged) ---
    private void handleTaskFailure(String dagId, String taskName) {
        try {
            String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
            if (dagJson == null) { return; }
            JsonNode dagPayload = objectMapper.readTree(dagJson);
            JsonNode taskNode = findTaskNode(dagPayload, taskName);
            if (taskNode == null) { return; }

            int maxRetries = taskNode.path("retries").path("count").asInt(0);
            String attemptKey = String.format("dag:%s:task:%s:attempts", dagId, taskName);
            long attemptsMade = redisTemplate.opsForValue().increment(attemptKey); // Increments and returns the new value

            if (attemptsMade <= maxRetries + 1) { // We check <= maxRetries + 1 because the first attempt is 1
                if(attemptsMade <= maxRetries) { // This means we have retries left
                    LOGGER.warn("Task '{}' FAILED on attempt {}. Re-dispatching for retry... (Max retries: {})", taskName, attemptsMade, maxRetries);
                    dispatchTask(dagId, taskNode);
                } else { // This means attemptsMade == maxRetries + 1, which was the final attempt
                    LOGGER.error("Task '{}' FAILED on final attempt {}. Initiating failure propagation.", taskName, attemptsMade -1); // Log the attempt number that failed
                    redisTemplate.opsForValue().set(String.format("dag:%s:task:%s:status", dagId, taskName), "FAILED");
                    propagateFailure(dagId, taskName);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Unexpected error during failure handling for task '{}', DAG '{}'", taskName, dagId, e);
        }
    }

    // --- propagateFailure (Refined logic - Unchanged) ---
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
                            if ("PENDING".equals(currentChildStatus)) {
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

    // --- getDagStatus (Unchanged) ---
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


    // --- HELPER METHODS (Unchanged) ---
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
        List<String> command = objectMapper.convertValue(taskNode.get("command"), new TypeReference<List<String>>() {});
        String taskName = taskNode.get("name").asText();
        String cleanTaskNameK8s = taskName.replaceAll("[^A-Za-z0-9\\-_.]", "").toLowerCase();
        cleanTaskNameK8s = cleanTaskNameK8s.substring(0, Math.min(cleanTaskNameK8s.length(), 63)).replaceAll("^-|-$", "");
        String cleanDagIdK8s = dagId.replaceAll("[^A-Za-z0-9\\-_.]", "").toLowerCase();
        cleanDagIdK8s = cleanDagIdK8s.substring(0, Math.min(cleanDagIdK8s.length(), 63)).replaceAll("^-|-$", "");

        V1ObjectMeta jobMeta = new V1ObjectMeta()
                .name(jobName)
                .namespace("helios")
                .putLabelsItem("app", "helios-task")
                .putLabelsItem("helios-dag-id", cleanDagIdK8s)
                .putLabelsItem("helios-task-name", cleanTaskNameK8s);

        V1Container container = new V1Container()
                .name(cleanTaskNameK8s.substring(0, Math.min(cleanTaskNameK8s.length(), 50)) + "-cont")
                .image(image)
                .command(command);

        V1PodSpec podSpec = new V1PodSpec()
                .restartPolicy("Never")
                .addContainersItem(container);

        V1PodTemplateSpec template = new V1PodTemplateSpec()
                .metadata(new V1ObjectMeta()
                        .putLabelsItem("app", "helios-task-pod")
                        .putLabelsItem("job-name", jobName)
                        .putLabelsItem("helios-dag-id", cleanDagIdK8s)
                        .putLabelsItem("helios-task-name", cleanTaskNameK8s))
                .spec(podSpec);

        V1JobSpec jobSpec = new V1JobSpec()
                .ttlSecondsAfterFinished(3600)
                .backoffLimit(0)
                .template(template);

        return new V1Job()
                .apiVersion("batch/v1")
                .kind("Job")
                .metadata(jobMeta)
                .spec(jobSpec);
    }
}

