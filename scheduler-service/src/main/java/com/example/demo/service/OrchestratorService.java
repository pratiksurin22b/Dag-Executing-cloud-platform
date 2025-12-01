package com.example.demo.service;

import com.example.demo.config.RabbitMQConfig;
import com.example.demo.dto.DagStatusResponse;
import com.example.demo.dto.SystemStatusResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

// Kubernetes Imports
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Streams;
import io.kubernetes.client.informer.SharedInformerFactory;
import io.kubernetes.client.informer.cache.Indexer;
import io.kubernetes.client.informer.ResourceEventHandler;
import io.kubernetes.client.util.CallGeneratorParams;
import okhttp3.Call;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * The OrchestratorService is the "brain" managing DAG execution via Kubernetes Jobs.
 * MERGED VERSION: Includes original robust monitoring + New Node-Level Caching.
 */
@Service
public class OrchestratorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrchestratorService.class);
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;
    private final MinioClient minioClient; // NEW: For size calculations
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

    // --- CACHING CONSTANTS ---
    private static final String ARTIFACTS_DIR = "/artifacts";
    private static final String CACHE_HOST_PATH = "/var/helios/ram-cache"; // Matches DaemonSet
    private static final String CACHE_MOUNT_PATH = "/cache";
    private static final String BUCKET_NAME = "helios-artifacts";
    private static final long RAM_CACHE_LIMIT = 536_870_912L; // 512 MB Limit for RAM

    @Autowired
    public OrchestratorService(StringRedisTemplate redisTemplate,
                               ObjectMapper objectMapper,
                               MinioClient minioClient) throws IOException { // Added MinioClient
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        this.minioClient = minioClient;

        ApiClient client;
        try {
            String k8sHost = System.getenv("KUBERNETES_SERVICE_HOST");
            if (k8sHost != null && !k8sHost.isBlank()) {
                client = ClientBuilder.cluster().build();
                LOGGER.info("Initialized Kubernetes ApiClient using in-cluster configuration (host={}).", k8sHost);
            } else {
                client = ClientBuilder.defaultClient();
                LOGGER.warn("KUBERNETES_SERVICE_HOST not set; initialized ApiClient using default kubeconfig.");
            }
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Kubernetes ApiClient. Root cause: {}", e.getMessage(), e);
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

    private void setupJobWatcher() {
        var jobInformer = informerFactory.sharedIndexInformerFor(
                (CallGeneratorParams params) -> {
                    try {
                        return batchV1Api.listNamespacedJobCall(
                                "helios", null, null, null, null,
                                "app=helios-task", null, params.resourceVersion, null,
                                params.timeoutSeconds, params.watch, null
                        );
                    } catch (ApiException e) {
                        LOGGER.error("Watcher: Failed to create list call for Jobs: {}", e.getResponseBody(), e);
                        throw new RuntimeException(e);
                    }
                },
                V1Job.class, V1JobList.class
        );

        jobInformer.addEventHandler(new ResourceEventHandler<V1Job>() {
            @Override public void onAdd(V1Job job) { }
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
                        }
                    }
                }
            }
            @Override public void onDelete(V1Job job, boolean deletedFinalStateUnknown) { }
        });
        this.jobIndexer = jobInformer.getIndexer();
    }

    @PostConstruct
    public void startWatchers() {
        String k8sHost = System.getenv("KUBERNETES_SERVICE_HOST");
        if (k8sHost == null || k8sHost.isBlank()) {
            LOGGER.warn("KUBERNETES_SERVICE_HOST not set; skipping watchers.");
            return;
        }
        LOGGER.info("Starting Kubernetes watchers...");
        informerFactory.startAllRegisteredInformers();
        LOGGER.info("Kubernetes watchers started.");
    }

    // --- YOUR ORIGINAL ROBUST SYSTEM STATUS METHOD (Restored) ---
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
            if (!hasDeploymentOrSts) {
                desiredReplicas = matchingPods.isEmpty() ? 0 : 1;
            }

            int readyReplicas = 0;
            int notReadyReplicas = 0;
            int pendingReplicas = 0;
            int crashLoopingReplicas = 0;
            int imagePullBackOffReplicas = 0;
            int runningPhaseReplicas = 0;
            int unresponsiveReplicas = 0;

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
                        if ("True".equalsIgnoreCase(c.getStatus()) && c.getType() != null) {
                            trueConditions.add(c.getType());
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
                    if (waitingReasons.isEmpty()) unresponsiveReplicas++;
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
                    serviceName, status, runningPhaseReplicas, desiredReplicas,
                    firstRunningPodName, readyReplicas, notReadyReplicas, pendingReplicas,
                    crashLoopingReplicas, imagePullBackOffReplicas, podBriefs
            ));
        }
        return new SystemStatusResponse(serviceStatuses);
    }


    public String processNewDagSubmission(JsonNode dagPayload) {
        String dagId = "dag-" + UUID.randomUUID();
        try {
            redisTemplate.opsForValue().set("dag:" + dagId + ":definition", dagPayload.toString());
            boolean useCache = dagPayload.has("useCache") && dagPayload.get("useCache").asBoolean(false);
            redisTemplate.opsForValue().set("dag:" + dagId + ":useCache", Boolean.toString(useCache));

            long startMillis = System.currentTimeMillis();
            String dagName = dagPayload.has("dagName") ? dagPayload.get("dagName").asText() : dagId;
            String metricsKey = "dag:" + dagId + ":metrics";
            redisTemplate.opsForHash().put(metricsKey, "dagId", dagId);
            redisTemplate.opsForHash().put(metricsKey, "dagName", dagName);
            redisTemplate.opsForHash().put(metricsKey, "startTime", Long.toString(startMillis));
            redisTemplate.opsForHash().put(metricsKey, "status", "RUNNING");
            redisTemplate.opsForHash().put(metricsKey, "cacheEnabled", Boolean.toString(useCache));

            String runsListKey = "dagRuns:" + dagName;
            redisTemplate.opsForList().leftPush(runsListKey, dagId);
            redisTemplate.opsForList().trim(runsListKey, 0, 49);

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

    // --- DISPATCH TASK (UPDATED WITH AFFINITY) ---
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
            boolean useCache = Boolean.parseBoolean(redisTemplate.opsForValue().get("dag:" + dagId + ":useCache"));

            // NEW: Calculate Best Node based on Data Gravity
            String preferredNode = calculateBestNode(dagId, taskNode);
            if (preferredNode != null) {
                LOGGER.info("Optimized Scheduling: Task '{}' prefers node '{}' due to data locality.", taskName, preferredNode);
            }

            // NEW: Pass preferredNode to Job Creator
            V1Job job = createK8sJobDefinition(jobName, dagId, taskNode,
                    inputArtifactsSpec, outputArtifactsSpec, useCache, preferredNode);

            LOGGER.info("Submitting Kubernetes Job '{}' for task '{}' (Attempt {})...", jobName, taskName, nextAttempt);
            batchV1Api.createNamespacedJob("helios", job, null, null, null, null);

            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "K8S_JOB_SUBMITTED");

        } catch (Exception e) {
            LOGGER.error("Failed to dispatch task '{}'", taskName, e);
            redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":status", "DISPATCH_FAILED");
            handleTaskFailureInternal(dagId, taskName);
        }
    }

    // NEW: "Data Gravity" Logic
    private String calculateBestNode(String dagId, JsonNode taskNode) {
        if (!taskNode.has("depends_on")) return null;
        Map<String, Long> nodeDataGravity = new HashMap<>();

        for (JsonNode dep : taskNode.get("depends_on")) {
            String parentTask = dep.asText();
            // Where did the parent run?
            String parentNode = redisTemplate.opsForValue().get(String.format("dag:%s:task:%s:node", dagId, parentTask));
            // How much data did it produce?
            String sizeStr = redisTemplate.opsForValue().get(String.format("dag:%s:task:%s:size", dagId, parentTask));
            long size = (sizeStr != null) ? Long.parseLong(sizeStr) : 0;

            if (parentNode != null) {
                nodeDataGravity.put(parentNode, nodeDataGravity.getOrDefault(parentNode, 0L) + size);
            }
        }
        // Return node with the most data
        return nodeDataGravity.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
    }

    // --- JOB DEFINITION (UPDATED WITH RAM CACHE & AFFINITY) ---
    private V1Job createK8sJobDefinition(String jobName, String dagId, JsonNode taskNode,
                                         String inputArtifactsSpec,
                                         String outputArtifactsSpec,
                                         boolean useCache,
                                         String preferredNode) throws IOException {
        String image = taskNode.get("image").asText();
        List<String> command = objectMapper.convertValue(taskNode.get("command"), new TypeReference<List<String>>() {});
        String taskName = taskNode.get("name").asText();
        String cleanDagId = dagId.replaceAll("[^a-z0-9-]", "").toLowerCase();
        String cleanTaskName = taskName.replaceAll("[^a-z0-9-]", "").toLowerCase();

        V1ObjectMeta jobMeta = new V1ObjectMeta()
                .name(jobName)
                .namespace("helios")
                .putLabelsItem("app", "helios-task")
                .putLabelsItem("helios-dag-id", cleanDagId)
                .putLabelsItem("helios-task-name", cleanTaskName)
                .putAnnotationsItem("helios/cache-enabled", Boolean.toString(useCache));

        // Volumes
        V1Volume artifactsVolume = new V1Volume().name("artifacts-workdir").emptyDir(new V1EmptyDirVolumeSource());
        V1VolumeMount artifactsMount = new V1VolumeMount().name("artifacts-workdir").mountPath(ARTIFACTS_DIR);
        V1Volume cacheVolume = new V1Volume().name("artifact-cache").hostPath(new V1HostPathVolumeSource().path(CACHE_HOST_PATH).type("DirectoryOrCreate"));
        V1VolumeMount cacheMount = new V1VolumeMount().name("artifact-cache").mountPath(CACHE_MOUNT_PATH);

        // Environment Variables
        List<V1EnvVar> env = new ArrayList<>();
        env.add(new V1EnvVar().name("HELIOS_DAG_ID").value(dagId));
        env.add(new V1EnvVar().name("HELIOS_TASK_NAME").value(taskName));
        env.add(new V1EnvVar().name("HELIOS_ARTIFACTS_DIR").value(ARTIFACTS_DIR));
        env.add(new V1EnvVar().name("HELIOS_CACHE_DIR").value(CACHE_MOUNT_PATH));
        env.add(new V1EnvVar().name("HELIOS_INPUT_ARTIFACTS").value(inputArtifactsSpec));
        env.add(new V1EnvVar().name("HELIOS_OUTPUT_ARTIFACTS").value(outputArtifactsSpec));
        env.add(new V1EnvVar().name("HELIOS_CACHE_ENABLED").value(Boolean.toString(useCache)));
        env.add(new V1EnvVar().name("HELIOS_ARTIFACTS_ENDPOINT").value(System.getenv().getOrDefault("HELIOS_ARTIFACTS_ENDPOINT", "http://minio:9000")));
        env.add(new V1EnvVar().name("HELIOS_ARTIFACTS_BUCKET").value(BUCKET_NAME));
        env.add(new V1EnvVar().name("HELIOS_ARTIFACTS_ACCESS_KEY").value(System.getenv().getOrDefault("HELIOS_ARTIFACTS_ACCESS_KEY", "admin")));
        env.add(new V1EnvVar().name("HELIOS_ARTIFACTS_SECRET_KEY").value(System.getenv().getOrDefault("HELIOS_ARTIFACTS_SECRET_KEY", "password")));

        // --- 1. SMART DOWNLOADER (INIT SCRIPT) ---
        // Logic: If Cache Enabled -> Check RAM -> Spin Lock -> Download to RAM.
        //        If Cache Disabled -> FORCE download from MinIO to Workspace.
        String initScript = String.join("\n",
                "set -e",
                "echo \"$HELIOS_INPUT_ARTIFACTS\" | while IFS=';' read -r ARTIFACT_PAIRS; do",
                "  for pair in $ARTIFACT_PAIRS; do",
                "    [ -z \"$pair\" ] && continue",
                "    name=\"${pair%%=*}\"",
                "    key=\"${pair#*=}\"",
                "    workspacePath=\"$HELIOS_ARTIFACTS_DIR/$name\"",
                "    cachePath=\"$HELIOS_CACHE_DIR/global/$key\"",
                "    lockDir=\"$cachePath.lock\"",
                "    mkdir -p \"$(dirname \"$cachePath\")\" \"$(dirname \"$workspacePath\")\"",

                "    if [ \"$HELIOS_CACHE_ENABLED\" = 'true' ]; then",
                "      # --- CACHE ENABLED PATH ---",
                "      if [ -f \"$cachePath\" ]; then",
                "        echo \"[HELIOS] RAM Cache HIT for '$key'. Copying...\"",
                "        cp -f \"$cachePath\" \"$workspacePath\"",
                "        continue",
                "      fi",
                "      echo \"[HELIOS] RAM Cache MISS for '$key'. Acquiring lock...\"",
                "      if mkdir \"$lockDir\" 2>/dev/null; then",
                "        tmpFile=\"$cachePath.tmp\"",
                "        if curl -sS --fail --retry 3 \"$HELIOS_ARTIFACTS_ENDPOINT/$HELIOS_ARTIFACTS_BUCKET/$key\" -o \"$tmpFile\"; then",
                "          mv \"$tmpFile\" \"$cachePath\"; rmdir \"$lockDir\"",
                "          echo \"[HELIOS] Downloaded to cache.\"",
                "        else rmdir \"$lockDir\"; exit 1; fi",
                "      else",
                "        while [ -d \"$lockDir\" ]; do sleep 1; done",
                "      fi",
                "      [ -f \"$cachePath\" ] && cp -f \"$cachePath\" \"$workspacePath\"",
                "    else",
                "      # --- CACHE DISABLED PATH (STRICT FORCE DOWNLOAD) ---",
                "      echo \"[HELIOS] Cache Disabled. Forcing download from MinIO...\"",
                "      curl -sS --fail --retry 3 \"$HELIOS_ARTIFACTS_ENDPOINT/$HELIOS_ARTIFACTS_BUCKET/$key\" -o \"$workspacePath\"",
                "    fi",
                "  done",
                "done"
        );

        V1Container initContainer = new V1Container()
                .name("init-artifacts").image("curlimages/curl:8.10.1")
                .command(List.of("/bin/sh", "-c", initScript))
                .volumeMounts(List.of(artifactsMount, cacheMount))
                .env(env);

        // --- 2. SMART UPLOADER (WRAPPER SCRIPT) ---
        // Logic: If Cache Enabled -> Write to RAM if small enough -> Upload MinIO.
        //        If Cache Disabled -> Skip RAM -> Upload MinIO directly.
        StringBuilder wrapperScript = new StringBuilder();
        wrapperScript.append("set -e\n");
        if (command != null && !command.isEmpty()) wrapperScript.append(String.join(" ", command)).append("\n");

        wrapperScript.append("if [ -n \"$HELIOS_OUTPUT_ARTIFACTS\" ]; then\n");
        wrapperScript.append("  echo \"$HELIOS_OUTPUT_ARTIFACTS\" | while IFS=';' read -r PAIRS; do\n");
        wrapperScript.append("    for pair in $PAIRS; do\n");
        wrapperScript.append("      name=\"${pair%%=*}\"; key=\"${pair#*=}\"\n");
        wrapperScript.append("      localPath=\"$HELIOS_ARTIFACTS_DIR/$name\"\n");
        wrapperScript.append("      cachePath=\"$HELIOS_CACHE_DIR/global/$key\"\n");
        wrapperScript.append("      if [ -f \"$localPath\" ]; then\n");

        // Strict check: Only write to RAM if enabled
        wrapperScript.append("        if [ \"$HELIOS_CACHE_ENABLED\" = 'true' ]; then\n");
        wrapperScript.append("          size=$(stat -c%s \"$localPath\")\n");
        wrapperScript.append("          if [ $size -lt " + RAM_CACHE_LIMIT + " ]; then\n");
        wrapperScript.append("            mkdir -p \"$(dirname \"$cachePath\")\"\n");
        wrapperScript.append("            cp -f \"$localPath\" \"$cachePath\"\n");
        wrapperScript.append("            echo \"[HELIOS] Cached '$name' to RAM.\"\n");
        wrapperScript.append("          else echo \"[HELIOS] Too large for RAM. Skipping.\"; fi\n");
        wrapperScript.append("        fi\n");

        // Always upload to MinIO
        wrapperScript.append("        curl -sS --fail -T \"$localPath\" \"$HELIOS_ARTIFACTS_ENDPOINT/$HELIOS_ARTIFACTS_BUCKET/$key\"\n");
        wrapperScript.append("      fi\n");
        wrapperScript.append("    done\n");
        wrapperScript.append("  done\n");
        wrapperScript.append("fi\n");

        V1Container mainContainer = new V1Container()
                .name("main").image(image)
                .command(List.of("/bin/sh", "-c", wrapperScript.toString()))
                .volumeMounts(List.of(artifactsMount, cacheMount))
                .env(env);

        V1Affinity affinity = null;
        if (preferredNode != null) {
            affinity = new V1Affinity().nodeAffinity(new V1NodeAffinity()
                    .addPreferredDuringSchedulingIgnoredDuringExecutionItem(new V1PreferredSchedulingTerm()
                            .weight(100)
                            .preference(new V1NodeSelectorTerm().addMatchExpressionsItem(new V1NodeSelectorRequirement()
                                    .key("kubernetes.io/hostname").operator("In").addValuesItem(preferredNode)))));
        }

        return new V1Job()
                .apiVersion("batch/v1").kind("Job").metadata(jobMeta)
                .spec(new V1JobSpec().template(new V1PodTemplateSpec()
                        .metadata(new V1ObjectMeta().labels(jobMeta.getLabels()).putLabelsItem("job-name", jobName))
                        .spec(new V1PodSpec().restartPolicy("Never").affinity(affinity)
                                .addVolumesItem(artifactsVolume).addVolumesItem(cacheVolume)
                                .addInitContainersItem(initContainer).addContainersItem(mainContainer))));
    }
    // --- HANDLE JOB COMPLETION (UPDATED) ---
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

        // NEW: Get Node Name
        String nodeName = fetchNodeNameForJob(jobName, "helios");

        processJobResult(dagId, taskName, succeeded, logs, nodeName);
    }

    // NEW: Fetch Node Name Helper
    private String fetchNodeNameForJob(String jobName, String namespace) {
        try {
            V1PodList list = coreV1Api.listNamespacedPod(namespace, null, null, null, null,
                    "job-name=" + jobName, null, null, null, null, null);
            if (list != null && !list.getItems().isEmpty()) {
                return list.getItems().get(0).getSpec().getNodeName();
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to determine execution node for job '{}'", jobName);
        }
        return null;
    }

    // --- YOUR ORIGINAL ROBUST LOG FETCH (Restored) ---
    private String fetchPodLogsForJob(String jobName, String namespace) {
        int maxAttempts = 10;
        long initialBackoffMs = 500L;
        long maxBackoffMs = 3000L;
        long maxTotalWaitMs = 30000L;
        return fetchPodLogsWithRetry(jobName, namespace, maxAttempts, initialBackoffMs, maxBackoffMs, maxTotalWaitMs);
    }

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
            if (elapsed > maxTotalWaitMs) break;

            try {
                String labelSelector = "job-name=" + jobName;
                V1PodList podList = coreV1Api.listNamespacedPod(
                        namespace, null, null, null, null, labelSelector,
                        null, null, null, null, null
                );

                if (podList == null || podList.getItems() == null || podList.getItems().isEmpty()) {
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
                            containerName = pod.getSpec().getContainers().get(0).getName();
                        }
                        if (containerName == null) return "Error: Container name not found for pod '" + podName + "'.";

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
                            LOGGER.debug("Log fetch attempt {} for job '{}' - pod '{}' phase='{}', initializing={}. Retrying...",
                                    attempt, jobName, podName, podPhase, initializing);
                        } else {
                            try {
                                String logContent = coreV1Api.readNamespacedPodLog(
                                        podName, namespace, containerName, Boolean.FALSE, Boolean.FALSE,
                                        (Integer) null, (String) null, Boolean.FALSE, (Integer) null, (Integer) null, Boolean.FALSE
                                );
                                return logContent != null ? logContent : "";
                            } catch (ApiException e) {
                                lastApiException = e;
                                if (e.getCode() == 400 || e.getCode() == 404 || e.getCode() == 409 || e.getCode() >= 500) {
                                    LOGGER.warn("K8s API error fetching logs: {} (code {}). Will retry.", e.getResponseBody(), e.getCode());
                                } else {
                                    return "Error fetching logs: API Error " + e.getCode();
                                }
                            }
                        }
                    }
                }
            } catch (ApiException e) {
                lastApiException = e;
                LOGGER.warn("K8s API error listing pods: {} (code {}).", e.getResponseBody(), e.getCode());
            } catch (Exception e) {
                lastGenericException = e;
                LOGGER.warn("Unexpected error during log fetch: {}", e.getMessage());
            }

            if (attempt < maxAttempts) {
                try {
                    long sleepMs = Math.min(backoff, maxBackoffMs);
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                backoff = Math.min(backoff * 2, maxBackoffMs);
            }
        }
        return "Pod logs not available after waiting " + (System.currentTimeMillis() - startTime) + " ms";
    }

    private void processJobResult(String dagId, String taskName, boolean success, String logs, String nodeName) {
        try {
            String taskStatusKey = String.format("dag:%s:task:%s:status", dagId, taskName);
            String taskLogsKey = String.format("dag:%s:task:%s:logs", dagId, taskName);
            List<String> logList = (logs != null && !logs.startsWith("Error:")) ? List.of(logs.split("\n")) : List.of(logs);
            redisTemplate.opsForValue().set(taskLogsKey, objectMapper.writeValueAsString(logList));

            // --- FIX FOR UI CACHE HIT ---
            // We check the logs for our specific success message.
            if (logs != null && logs.contains("[HELIOS] RAM Cache HIT")) {
                String metricsKey = "dag:" + dagId + ":metrics";
                redisTemplate.opsForHash().put(metricsKey, "cacheHit", "true");
                LOGGER.info("Metrics updated: Cache HIT detected for task '{}'", taskName);
            }
            // ---------------------------

            if (success) {
                redisTemplate.opsForValue().set(taskStatusKey, "SUCCEEDED");
                LOGGER.info("Task '{}' SUCCEEDED on node '{}'.", taskName, nodeName);

                if (nodeName != null) {
                    redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":node", nodeName);

                    long totalSize = 0;
                    try {
                        String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
                        JsonNode dagPayload = objectMapper.readTree(dagJson);
                        JsonNode taskNode = findTaskNode(dagPayload, taskName);
                        if (taskNode != null && taskNode.has("outputs")) {
                            for (JsonNode out : taskNode.get("outputs")) {
                                String key = String.format("dags/%s/tasks/%s/outputs/%s", dagId, taskName, out.asText());
                                totalSize += minioClient.statObject(StatObjectArgs.builder()
                                        .bucket(BUCKET_NAME).object(key).build()).size();
                            }
                        }
                        redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + taskName + ":size", String.valueOf(totalSize));
                    } catch (Exception e) {
                        LOGGER.warn("Failed to update metadata for task {}", taskName, e);
                    }
                }
                evaluateDag(dagId);
            } else {
                handleTaskFailure(dagId, taskName);
            }
            finalizeDagMetricsIfComplete(dagId);
        } catch (Exception e) {
            LOGGER.error("Error processing result for task '{}'", taskName, e);
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

            if (allTerminal) {
                long endMillis = System.currentTimeMillis();
                String metricsKey = "dag:" + dagId + ":metrics";
                Object startTimeRaw = redisTemplate.opsForHash().get(metricsKey, "startTime");
                long startMillis = (startTimeRaw != null) ? Long.parseLong(startTimeRaw.toString()) : endMillis;
                long durationMs = Math.max(0, endMillis - startMillis);

                redisTemplate.opsForHash().put(metricsKey, "endTime", Long.toString(endMillis));
                redisTemplate.opsForHash().put(metricsKey, "durationMs", Long.toString(durationMs));
                redisTemplate.opsForHash().put(metricsKey, "status", anyFailed ? "FAILED" : "SUCCEEDED");
            }
        } catch (Exception e) {
            LOGGER.error("Failed to finalize DAG metrics for DAG ID: {}", dagId, e);
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

    private String buildOutputArtifactsSpec(String dagId, JsonNode taskNode) {
        if (!taskNode.has("outputs") || !taskNode.get("outputs").isArray()) return "";
        String taskName = taskNode.get("name").asText();
        List<String> specs = new ArrayList<>();
        for (JsonNode out : taskNode.get("outputs")) {
            String name = out.asText();
            String key = String.format("dags/%s/tasks/%s/outputs/%s", dagId, taskName, name);
            specs.add(name + "=" + key);
        }
        return String.join(";", specs);
    }

    private String buildInputArtifactsSpec(String dagId, JsonNode taskNode) {
        if (!taskNode.has("inputs") || !taskNode.get("inputs").isArray()) return "";
        List<String> specs = new ArrayList<>();
        for (JsonNode in : taskNode.get("inputs")) {
            String fromTask = in.path("fromTask").asText(null);
            String name = in.path("name").asText(null);
            if (fromTask != null && name != null) {
                String key = String.format("dags/%s/tasks/%s/outputs/%s", dagId, fromTask, name);
                specs.add(name + "=" + key);
            }
        }
        return String.join(";", specs);
    }

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
                    LOGGER.warn("Task '{}' FAILED on attempt {}. Re-dispatching...", taskName, attemptsMade);
                    dispatchTask(dagId, taskNode);
                } else {
                    LOGGER.error("Task '{}' FAILED on final attempt.", taskName);
                    redisTemplate.opsForValue().set(String.format("dag:%s:task:%s:status", dagId, taskName), "FAILED");
                    propagateFailure(dagId, taskName);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error during failure handling for task '{}'", taskName, e);
        }
    }

    public void handleTaskFailure(String dagId, String taskName) {
        handleTaskFailureInternal(dagId, taskName);
    }

    public void propagateFailure(String dagId, String failedTaskName) {
        try {
            String dagJson = redisTemplate.opsForValue().get("dag:" + dagId + ":definition");
            if (dagJson == null) return;
            JsonNode dagPayload = objectMapper.readTree(dagJson);
            Set<String> failedTasks = new HashSet<>();
            failedTasks.add(failedTaskName);
            boolean changed = true;
            while (changed) {
                changed = false;
                for (JsonNode task : dagPayload.get("tasks")) {
                    String tName = task.get("name").asText();
                    if (!failedTasks.contains(tName) && task.has("depends_on")) {
                        for (JsonNode dep : task.get("depends_on")) {
                            if (failedTasks.contains(dep.asText())) {
                                failedTasks.add(tName);
                                redisTemplate.opsForValue().set("dag:" + dagId + ":task:" + tName + ":status", "UPSTREAM_FAILED");
                                changed = true;
                                break;
                            }
                        }
                    }
                }
            }
            finalizeDagMetricsIfComplete(dagId);
        } catch (Exception e) { LOGGER.error("Propagate failure error", e); }
    }

    public List<Map<String, Object>> getDagRunMetrics(String dagName, int limit) {
        String runsListKey = "dagRuns:" + dagName;
        List<String> dagIds = redisTemplate.opsForList().range(runsListKey, 0, limit - 1);
        if (dagIds == null || dagIds.isEmpty()) return Collections.emptyList();

        List<Map<String, Object>> runs = new ArrayList<>();
        for (String dagId : dagIds) {
            String metricsKey = "dag:" + dagId + ":metrics";
            Map<Object, Object> raw = redisTemplate.opsForHash().entries(metricsKey);
            if (!raw.isEmpty()) {
                Map<String, Object> run = raw.entrySet().stream()
                        .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
                run.put("dagId", dagId);
                runs.add(run);
            }
        }
        runs.sort((a, b) -> {
            long sa = Long.parseLong(String.valueOf(a.getOrDefault("startTime", "0")));
            long sb = Long.parseLong(String.valueOf(b.getOrDefault("startTime", "0")));
            return Long.compare(sb, sa);
        });
        return runs;
    }
}