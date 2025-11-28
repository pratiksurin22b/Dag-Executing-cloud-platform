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
import java.util.concurrent.atomic.AtomicInteger;
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
            "Redis", "redis",
            "RabbitMQ", "rabbitmq",
            "MinIO", "minio"
    );

    // Base directory inside task containers for artifact files
    private static final String ARTIFACTS_DIR = "/artifacts";
    // Node-local cache configuration (hostPath mounted into pods)
    private static final String CACHE_HOST_PATH = "/var/helios/cache";
    private static final String CACHE_MOUNT_PATH = "/cache";


    @Autowired
    public OrchestratorService(StringRedisTemplate redisTemplate, ObjectMapper objectMapper) throws IOException {
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;

        ApiClient client;
        try {
            // Prefer in-cluster config when running inside Kubernetes (KUBERNETES_SERVICE_HOST is set)
            String k8sHost = System.getenv("KUBERNETES_SERVICE_HOST");
            if (k8sHost != null && !k8sHost.isBlank()) {
                client = ClientBuilder.cluster().build();
                LOGGER.info("Initialized Kubernetes ApiClient using in-cluster configuration (host={}).", k8sHost);
            } else {
                // When running outside a cluster (e.g., docker-compose or local dev), fall back to
                // the default client which reads ~/.kube/config or KUBECONFIG if available.
                client = ClientBuilder.defaultClient();
                LOGGER.warn("KUBERNETES_SERVICE_HOST not set; initialized ApiClient using default kubeconfig. " +
                        "If no kubeconfig is present, Kubernetes operations may fail.");
            }
        } catch (Exception e) {
            // As a last resort, build a basic ApiClient with no server; log loudly so it’s visible.
            LOGGER.error("Failed to initialize Kubernetes ApiClient from cluster/default config. " +
                    "Orchestrator will not be able to talk to Kubernetes. Root cause: {}", e.getMessage(), e);
            client = new ApiClient();
        }

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
        String k8sHost = System.getenv("KUBERNETES_SERVICE_HOST");
        if (k8sHost == null || k8sHost.isBlank()) {
            LOGGER.warn("KUBERNETES_SERVICE_HOST not set; skipping Kubernetes watchers in non-cluster environment.");
            return;
        }
        LOGGER.info("Starting Kubernetes watchers...");
        informerFactory.startAllRegisteredInformers();
        LOGGER.info("Kubernetes watchers started.");
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
            // Persist raw DAG definition
            redisTemplate.opsForValue().set("dag:" + dagId + ":definition", dagPayload.toString());

            // NEW: Extract caching flag from payload (default false)
            boolean useCache = dagPayload.has("useCache") && dagPayload.get("useCache").asBoolean(false);
            redisTemplate.opsForValue().set("dag:" + dagId + ":useCache", Boolean.toString(useCache));

            // NEW: Initialize basic DAG-run metrics
            long startMillis = System.currentTimeMillis();
            String dagName = dagPayload.has("dagName") ? dagPayload.get("dagName").asText() : dagId;
            String metricsKey = "dag:" + dagId + ":metrics";
            redisTemplate.opsForHash().put(metricsKey, "dagId", dagId);
            redisTemplate.opsForHash().put(metricsKey, "dagName", dagName);
            redisTemplate.opsForHash().put(metricsKey, "startTime", Long.toString(startMillis));
            redisTemplate.opsForHash().put(metricsKey, "status", "RUNNING");
            redisTemplate.opsForHash().put(metricsKey, "cacheEnabled", Boolean.toString(useCache));

            // Maintain per-dagName list of recent runs (for UI metrics view)
            String runsListKey = "dagRuns:" + dagName;
            redisTemplate.opsForList().leftPush(runsListKey, dagId);
            redisTemplate.opsForList().trim(runsListKey, 0, 49); // keep last 50 runs

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

            String inputArtifactsSpec = buildInputArtifactsSpec(dagId, taskNode);
            String outputArtifactsSpec = buildOutputArtifactsSpec(dagId, taskNode);

            // Read caching flag from Redis and pass to job definition
            boolean useCache = Boolean.parseBoolean(
                    redisTemplate.opsForValue().get("dag:" + dagId + ":useCache")
            );

            V1Job job = createK8sJobDefinition(jobName, dagId, taskNode,
                    inputArtifactsSpec, outputArtifactsSpec, useCache);
            LOGGER.info("Submitting Kubernetes Job '{}' for task '{}' (Attempt {}, useCache={})...",
                    jobName, taskName, nextAttempt, useCache);
            V1Job createdJob = batchV1Api.createNamespacedJob(
                    "helios", job, null, null, null, null
            );
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "K8S_JOB_SUBMITTED");
            LOGGER.info("Successfully submitted Kubernetes Job '{}'.", createdJob.getMetadata().getName());

        } catch (ApiException e) {
            LOGGER.error("Kubernetes API Error dispatching task '{}': {}", taskName, e.getResponseBody(), e);
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "DISPATCH_FAILED");
            // If Job creation fails immediately, treat as a task failure to trigger propagation
            handleTaskFailureInternal(dagId, taskName);
        } catch (Exception e) {
            LOGGER.error("Failed to create/dispatch K8s Job for task '{}'", taskName, e);
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "DISPATCH_FAILED");
            handleTaskFailureInternal(dagId, taskName);
        }
    }

    // Build a semicolon-delimited mapping: name=minioKey;name2=minioKey2
    private String buildOutputArtifactsSpec(String dagId, JsonNode taskNode) {
        if (!taskNode.has("outputs") || !taskNode.get("outputs").isArray()) {
            return "";
        }
        String taskName = taskNode.get("name").asText();
        List<String> specs = new ArrayList<>();
        for (JsonNode out : taskNode.get("outputs")) {
            String name = out.asText();
            String key = String.format("dags/%s/tasks/%s/outputs/%s", dagId, taskName, name);
            specs.add(name + "=" + key);
        }
        return String.join(";", specs);
    }

    // Inputs expected as array of objects: [{"fromTask":"task1","name":"artifactName"}, ...]
    private String buildInputArtifactsSpec(String dagId, JsonNode taskNode) {
        if (!taskNode.has("inputs") || !taskNode.get("inputs").isArray()) {
            return "";
        }
        List<String> specs = new ArrayList<>();
        for (JsonNode in : taskNode.get("inputs")) {
            String fromTask = in.path("fromTask").asText(null);
            String name = in.path("name").asText(null);
            if (fromTask == null || name == null) {
                continue;
            }
            String key = String.format("dags/%s/tasks/%s/outputs/%s", dagId, fromTask, name);
            specs.add(name + "=" + key);
        }
        return String.join(";", specs);
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

    // --- fetchPodLogsForJob (v18 pattern) ---
    private String fetchPodLogsForJob(String jobName, String namespace) {
        // Keep the public signature simple and delegate to a retrying helper
        int maxAttempts = 10;
        long initialBackoffMs = 500L;
        long maxBackoffMs = 3000L;
        long maxTotalWaitMs = 30000L;
        return fetchPodLogsWithRetry(jobName, namespace, maxAttempts, initialBackoffMs, maxBackoffMs, maxTotalWaitMs);
    }

    /**
     * More robust log fetching that tolerates races where the Job is marked complete
     * but the Pod/container is still in PodInitializing or not yet ready for logs.
     * Always returns a non-null String (may be an explanatory error message).
     */
    private String fetchPodLogsWithRetry(String jobName,
                                         String namespace,
                                         int maxAttempts,
                                         long initialBackoffMs,
                                         long maxBackoffMs,
                                         long maxTotalWaitMs) {
        long startTime = System.currentTimeMillis();
        long backoff = initialBackoffMs;
        ApiException lastApiException = null;
        Exception lastGenericException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > maxTotalWaitMs) {
                break;
            }

            try {
                String labelSelector = "job-name=" + jobName;
                V1PodList podList = coreV1Api.listNamespacedPod(
                        namespace, null, null, null, null, labelSelector,
                        null, null, null, null, null
                );

                if (podList == null || podList.getItems() == null || podList.getItems().isEmpty()) {
                    // No pod yet for this job, backoff and retry
                    LOGGER.debug("Log fetch attempt {} for job '{}' - no pods found yet.", attempt, jobName);
                } else {
                    V1Pod pod = podList.getItems().get(0);
                    String podName = pod.getMetadata() != null ? pod.getMetadata().getName() : null;
                    String podPhase = pod.getStatus() != null ? pod.getStatus().getPhase() : "Unknown";

                    if (podName == null) {
                        LOGGER.warn("Log fetch attempt {} for job '{}' - pod without name.", attempt, jobName);
                    } else {
                        String containerName = null;
                        if (pod.getSpec() != null && pod.getSpec().getContainers() != null && !pod.getSpec().getContainers().isEmpty()) {
                            // Prefer the main task container if named, otherwise first container
                            containerName = pod.getSpec().getContainers().get(0).getName();
                        }
                        if (containerName == null) {
                            return "Error: Container name not found for pod '" + podName + "'.";
                        }

                        boolean terminalPhase = "Succeeded".equals(podPhase) || "Failed".equals(podPhase);
                        boolean initializing = false;
                        if (pod.getStatus() != null && pod.getStatus().getContainerStatuses() != null) {
                            for (V1ContainerStatus cStatus : pod.getStatus().getContainerStatuses()) {
                                if (cStatus.getState() != null && cStatus.getState().getWaiting() != null) {
                                    String reason = cStatus.getState().getWaiting().getReason();
                                    if (reason != null && reason.contains("PodInitializing")) {
                                        initializing = true;
                                        break;
                                    }
                                }
                            }
                        }

                        if (!terminalPhase || initializing) {
                            // Pod still starting or running; logs may not yet be readable. Retry.
                            LOGGER.debug(
                                    "Log fetch attempt {} for job '{}' - pod '{}' phase='{}', initializing={}. Retrying...",
                                    attempt, jobName, podName, podPhase, initializing
                            );
                        } else {
                            // Pod is in a terminal phase; try to read logs.
                            try {
                                String logContent = coreV1Api.readNamespacedPodLog(
                                        podName,
                                        namespace,
                                        containerName,
                                        Boolean.FALSE,
                                        Boolean.FALSE,
                                        (Integer) null,
                                        (String) null,
                                        Boolean.FALSE,
                                        (Integer) null,
                                        (Integer) null,
                                        Boolean.FALSE
                                );
                                return logContent != null ? logContent : "";
                            } catch (ApiException e) {
                                lastApiException = e;
                                // 400 BadRequest often means the container is not ready yet; retry those.
                                if (e.getCode() == 400 || e.getCode() == 404 || e.getCode() == 409 || e.getCode() >= 500) {
                                    LOGGER.warn("K8s API error fetching logs for job '{}' pod '{}': {} (code {}). Will retry if attempts remain.",
                                            jobName, podName, e.getResponseBody(), e.getCode());
                                } else {
                                    LOGGER.error("Non-retryable K8s API error fetching logs for job '{}' pod '{}': {} (code {}).",
                                            jobName, podName, e.getResponseBody(), e.getCode());
                                    return "Error fetching logs: API Error " + e.getCode();
                                }
                            }
                        }
                    }
                }
            } catch (ApiException e) {
                lastApiException = e;
                // For list pods failure, some codes may be transient.
                LOGGER.warn("K8s API error listing pods for job '{}': {} (code {}). Attempt {} of {}.",
                        jobName, e.getResponseBody(), e.getCode(), attempt, maxAttempts);
            } catch (Exception e) {
                lastGenericException = e;
                LOGGER.warn("Unexpected error during log fetch attempt {} for job '{}': {}", attempt, jobName, e.getMessage());
            }

            // Sleep before next attempt if there are attempts left
            if (attempt < maxAttempts) {
                try {
                    long sleepMs = Math.min(backoff, maxBackoffMs);
                    long remaining = maxTotalWaitMs - (System.currentTimeMillis() - startTime);
                    if (remaining <= 0) {
                        break;
                    }
                    sleepMs = Math.min(sleepMs, remaining);
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
                backoff = Math.min(backoff * 2, maxBackoffMs);
            }
        }

        // If we reach here, all retries are exhausted or total wait exceeded.
        if (lastApiException != null) {
            LOGGER.error("Exhausted log fetch retries for job '{}'. Last K8s API error: {} (code {}).",
                    jobName, lastApiException.getResponseBody(), lastApiException.getCode());
            return "Error fetching logs after retries: API Error " + lastApiException.getCode();
        }
        if (lastGenericException != null) {
            LOGGER.error("Exhausted log fetch retries for job '{}'. Last error: {}",
                    jobName, lastGenericException.getMessage(), lastGenericException);
            return "Error fetching logs after retries: " + lastGenericException.getMessage();
        }

        long totalElapsed = System.currentTimeMillis() - startTime;
        LOGGER.warn("Exhausted log fetch retries for job '{}' after {} ms without obtaining logs.", jobName, totalElapsed);
        return "Pod logs not available after waiting " + totalElapsed + " ms";
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

            // NEW: when all tasks have terminal status, finalize DAG metrics
            finalizeDagMetricsIfComplete(dagId);
        } catch (Exception e) {
            LOGGER.error("CRITICAL: Failed to process final job result for task '{}', DAG '{}'.", taskName, dagId, e);
        }
    }

    private void finalizeDagMetricsIfComplete(String dagId) {
        try {
            String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
            if (dagJson == null) { return; }
            JsonNode dagPayload = objectMapper.readTree(dagJson);

            boolean allTerminal = true;
            boolean anyFailed = false;
            for (JsonNode taskNode : dagPayload.get("tasks")) {
                String taskName = taskNode.get("name").asText();
                String status = redisTemplate.opsForValue().get(
                        String.format("dag:%s:task:%s:status", dagId, taskName));
                if (status == null || (!"SUCCEEDED".equals(status) && !"FAILED".equals(status) && !"UPSTREAM_FAILED".equals(status))) {
                    allTerminal = false;
                    break;
                }
                if ("FAILED".equals(status) || "UPSTREAM_FAILED".equals(status)) {
                    anyFailed = true;
                }
            }

            if (!allTerminal) {
                return; // still running
            }

            long endMillis = System.currentTimeMillis();
            String metricsKey = "dag:" + dagId + ":metrics";
            Object startTimeRaw = redisTemplate.opsForHash().get(metricsKey, "startTime");
            long startMillis = (startTimeRaw != null) ? Long.parseLong(startTimeRaw.toString()) : endMillis;
            long durationMs = Math.max(0, endMillis - startMillis);

            redisTemplate.opsForHash().put(metricsKey, "endTime", Long.toString(endMillis));
            redisTemplate.opsForHash().put(metricsKey, "durationMs", Long.toString(durationMs));
            redisTemplate.opsForHash().put(metricsKey, "status", anyFailed ? "FAILED" : "SUCCEEDED");
        } catch (Exception e) {
            LOGGER.error("Failed to finalize DAG metrics for DAG ID: {}", dagId, e);
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

    private V1Job createK8sJobDefinition(String jobName, String dagId, JsonNode taskNode,
                                         String inputArtifactsSpec,
                                         String outputArtifactsSpec,
                                         boolean useCache) throws IOException {
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
                .putLabelsItem("helios-task-name", cleanTaskNameK8s)
                .putAnnotationsItem("helios/cache-enabled", Boolean.toString(useCache));

        V1Volume artifactsVolume = new V1Volume()
                .name("artifacts-workdir")
                .emptyDir(new V1EmptyDirVolumeSource());

        V1VolumeMount artifactsMount = new V1VolumeMount()
                .name("artifacts-workdir")
                .mountPath(ARTIFACTS_DIR);

        // Node-local cache volume (hostPath)
        V1Volume cacheVolume = new V1Volume()
                .name("artifact-cache")
                .hostPath(new V1HostPathVolumeSource()
                        .path(CACHE_HOST_PATH)
                        .type("DirectoryOrCreate"));

        V1VolumeMount cacheMount = new V1VolumeMount()
                .name("artifact-cache")
                .mountPath(CACHE_MOUNT_PATH);

        // Shared env across containers
        List<V1EnvVar> commonEnv = Arrays.asList(
                new V1EnvVar().name("HELIOS_DAG_ID").value(dagId),
                new V1EnvVar().name("HELIOS_TASK_NAME").value(taskName),
                new V1EnvVar().name("HELIOS_ARTIFACTS_DIR").value(ARTIFACTS_DIR),
                new V1EnvVar().name("HELIOS_CACHE_DIR").value(CACHE_MOUNT_PATH),
                new V1EnvVar().name("HELIOS_INPUT_ARTIFACTS").value(inputArtifactsSpec),
                new V1EnvVar().name("HELIOS_OUTPUT_ARTIFACTS").value(outputArtifactsSpec),
                new V1EnvVar().name("HELIOS_ARTIFACTS_ENDPOINT").value(
                        System.getenv().getOrDefault("HELIOS_ARTIFACTS_ENDPOINT", "http://minio:9000")),
                new V1EnvVar().name("HELIOS_ARTIFACTS_BUCKET").value(
                        System.getenv().getOrDefault("HELIOS_ARTIFACTS_BUCKET", "helios-artifacts")),
                new V1EnvVar().name("HELIOS_ARTIFACTS_ACCESS_KEY").value(
                        System.getenv().getOrDefault("HELIOS_ARTIFACTS_ACCESS_KEY", "admin")),
                new V1EnvVar().name("HELIOS_ARTIFACTS_SECRET_KEY").value(
                        System.getenv().getOrDefault("HELIOS_ARTIFACTS_SECRET_KEY", "password")),
                new V1EnvVar().name("HELIOS_CACHE_ENABLED").value(Boolean.toString(useCache))
        );

        // Init container: smart downloader with cache
        String initScript = String.join("\n",
                "set -e",
                "echo '[HELIOS] Running init script version 3.0 (POSIX compliant).'",
                "echo '[HELIOS] Init container starting for task ' \"$HELIOS_TASK_NAME\"",
                "echo \"$HELIOS_INPUT_ARTIFACTS\" | while IFS=';' read -r ARTIFACT_PAIRS; do",
                "  for pair in $ARTIFACT_PAIRS; do",
                "    [ -z \"$pair\" ] && continue",
                "    name=\"${pair%%=*}\"",
                "    key=\"${pair#*=}\"",
                "    fileName=\"$(basename \"$key\")\"",
                "    workspacePath=\"$HELIOS_ARTIFACTS_DIR/$name\"",
                "    cachePath=\"$HELIOS_CACHE_DIR/$HELIOS_DAG_ID/$name/$fileName\"",
                "    mkdir -p \"$(dirname \"$cachePath\")\" \"$(dirname \"$workspacePath\")\"",
                "    if [ \"$HELIOS_CACHE_ENABLED\" = 'true' ] && [ -f \"$cachePath\" ]; then",
                "      echo '[HELIOS] Cache hit for ' \"$key\" ' -> ' \"$cachePath\"",
                "      cp -f \"$cachePath\" \"$workspacePath\"",
                "      continue",
                "    fi",
                "    echo '[HELIOS] Cache miss for ' \"$key\" '; downloading from MinIO...'",
                "    tmpFile=\"$cachePath.tmp\"",
                "    rm -f \"$tmpFile\"",
                "    curl -sS --fail --retry 3 --connect-timeout 30 \\",
                "      \"$HELIOS_ARTIFACTS_ENDPOINT/$HELIOS_ARTIFACTS_BUCKET/$key\" -o \"$tmpFile\" || {",
                "        echo '[HELIOS] ERROR: Download failed for ' \"$key\" ; exit 1; }",
                "    mv \"$tmpFile\" \"$cachePath\"",
                "    cp -f \"$cachePath\" \"$workspacePath\"",
                "  done",
                "done",
                "echo '[HELIOS] Init container completed.'"
        );

        V1Container initContainer = new V1Container()
                .name(cleanTaskNameK8s.substring(0, Math.min(cleanTaskNameK8s.length(), 40)) + "-init")
                .image("curlimages/curl:8.10.1")
                .addCommandItem("/bin/sh")
                .addArgsItem("-c")
                .addArgsItem(initScript)
                .addVolumeMountsItem(artifactsMount)
                .addVolumeMountsItem(cacheMount)
                .env(commonEnv);


        V1Container mainContainer = new V1Container()
                .name(cleanTaskNameK8s.substring(0, Math.min(cleanTaskNameK8s.length(), 50)) + "-cont")
                .image(image)
                .command(command)
                .addVolumeMountsItem(artifactsMount)
                .addVolumeMountsItem(cacheMount)
                .env(commonEnv);

        V1PodSpec podSpec = new V1PodSpec()
                .restartPolicy("Never")
                .addVolumesItem(artifactsVolume)
                .addVolumesItem(cacheVolume)
                .addInitContainersItem(initContainer)
                .addContainersItem(mainContainer);

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

    // Internal helper that encapsulates retry & failure propagation
    private void handleTaskFailureInternal(String dagId, String taskName) {
        try {
            String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
            if (dagJson == null) { return; }
            JsonNode dagPayload = objectMapper.readTree(dagJson);
            JsonNode taskNode = findTaskNode(dagPayload, taskName);
            if (taskNode == null) { return; }

            int maxRetries = taskNode.path("retries").path("count").asInt(0);
            String attemptKey = String.format("dag:%s:task:%s:attempts", dagId, taskName);
            long attemptsMade = redisTemplate.opsForValue().increment(attemptKey);

            if (attemptsMade <= maxRetries + 1) {
                if (attemptsMade <= maxRetries) {
                    LOGGER.warn("Task '{}' FAILED on attempt {}. Re-dispatching for retry... (Max retries: {})", taskName, attemptsMade, maxRetries);
                    dispatchTask(dagId, taskNode);
                } else {
                    LOGGER.error("Task '{}' FAILED on final attempt {}. Initiating failure propagation.", taskName, attemptsMade - 1);
                    redisTemplate.opsForValue().set(String.format("dag:%s:task:%s:status", dagId, taskName), "FAILED");
                    propagateFailure(dagId, taskName);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Unexpected error during failure handling for task '{}', DAG '{}'", taskName, dagId, e);
        }
    }

    // Public API used by other components like ResultListener
    public void handleTaskFailure(String dagId, String taskName) {
        handleTaskFailureInternal(dagId, taskName);
    }

    // Existing propagateFailure method should remain public
    public void propagateFailure(String dagId, String failedTaskName) {
        // ...existing propagateFailure implementation...
    }

    // Expose run-metrics retrieval for controller
    public List<Map<String, Object>> getDagRunMetrics(String dagName, int limit) {
        String runsListKey = "dagRuns:" + dagName;
        List<String> dagIds = redisTemplate.opsForList().range(runsListKey, 0, limit - 1);
        if (dagIds == null || dagIds.isEmpty()) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> runs = new ArrayList<>();
        for (String dagId : dagIds) {
            String metricsKey = "dag:" + dagId + ":metrics";
            Map<Object, Object> raw = redisTemplate.opsForHash().entries(metricsKey);
            if (raw == null || raw.isEmpty()) {
                continue;
            }
            Map<String, Object> run = raw.entrySet().stream()
                    .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
            run.put("dagId", dagId);
            runs.add(run);
        }
        runs.sort((a, b) -> {
            long sa = Long.parseLong(String.valueOf(a.getOrDefault("startTime", "0")));
            long sb = Long.parseLong(String.valueOf(b.getOrDefault("startTime", "0")));
            return Long.compare(sb, sa);
        });
        return runs;
    }
}
