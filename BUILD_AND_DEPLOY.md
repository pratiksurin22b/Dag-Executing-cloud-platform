# Build and Deploy Guide for Phase 2 Caching

This guide explains how to build and deploy the Phase 2 caching and metrics features.

## Prerequisites

- Docker installed and running
- Access to Docker Hub (or your container registry)
- kubectl configured to access your Kubernetes cluster
- Maven installed for building the scheduler-service
- Node.js and npm installed for building the frontend

## Build Steps

### 1. Build Scheduler Service

```bash
cd scheduler-service
mvn clean package -DskipTests
docker build -t pratik9634/scheduler-service:cache .
docker push pratik9634/scheduler-service:cache
```

### 2. Build Frontend

```bash
cd dag-ui-futuristic
npm install
npm run build
docker build -t pratik9634/frontend:cache .
docker push pratik9634/frontend:cache
```

### 3. Deploy to Kubernetes

```bash
# Deploy cache preparation DaemonSet (run once)
kubectl apply -f k8s/07-cache-prep-daemonset.yaml

# Update deployments to use new images
kubectl apply -f k8s/03-scheduler-service.yaml
kubectl apply -f k8s/04-frontend.yaml

# Verify deployments
kubectl get pods -n helios
kubectl get deployment -n helios

# Check images being used
kubectl get pod -n helios -l app=frontend -o "jsonpath={.items[0].spec.containers[0].image}"
kubectl get pod -n helios -l app=scheduler-service -o "jsonpath={.items[0].spec.containers[0].image}"
```

## Features Implemented

### Backend (scheduler-service)

1. **Node-level Caching**
   - HostPath volume at `/var/helios/cache` for persistent node-level cache
   - Smart downloader init container that:
     - Checks for cached artifacts before downloading
     - Downloads from MinIO on cache miss
     - Copies artifacts to both cache and workspace

2. **Metrics Tracking**
   - DAG run metrics stored in Redis
   - Tracks: dagId, dagName, startTime, endTime, durationMs, status, cacheEnabled, taskCount, taskSucceeded, taskFailed, taskUpstreamFailed, cacheHitTasks, cacheMissTasks, cacheHit, nodes
   - API endpoint: `GET /api/v1/dags/{dagName}/metrics?limit=N`

3. **Cache Control**
   - `useCache` flag in DAG payload
   - `HELIOS_CACHE_ENABLED` environment variable passed to jobs

### Frontend (dag-ui-futuristic)

1. **Cache Toggle**
   - Checkbox to enable/disable caching per DAG submission
   - Defaults to enabled

2. **Metrics Panel**
   - Summary cards showing:
     - Cached runs count and average duration
     - Non-cached runs count and average duration
     - Last run task load factor
   - Detailed metrics table with:
     - Run ID, Start Time, Duration, Cache Enabled, Cache Hit, Status, Tasks, Nodes
   - Automatically fetches metrics after DAG submission

### Kubernetes

1. **DaemonSet for Cache Prep**
   - Automatically creates `/var/helios/cache` on every node
   - Sets proper permissions (777 for write access)
   - Runs as privileged container with host root access

## Testing

### 1. Test Cache Functionality

Submit a DAG with artifacts twice to the same node:

```yaml
apiVersion: v1
dagName: "cache-test"
tasks:
  - name: "generate-data"
    image: "ubuntu:latest"
    command: ["/bin/bash", "-c", "echo 'test data' > /artifacts/output.txt"]
    outputs: ["output.txt"]
  
  - name: "process-data"
    image: "ubuntu:latest"
    depends_on: ["generate-data"]
    command: ["/bin/bash", "-c", "cat /artifacts/input.txt"]
    inputs:
      - fromTask: "generate-data"
        name: "input.txt"
```

First run: Should see "Cache miss" in init container logs
Second run: Should see "Cache hit" in init container logs

### 2. Test Metrics

1. Submit multiple DAG runs with cache enabled and disabled
2. Check the metrics panel in the UI
3. Verify metrics via API: `curl http://localhost:8080/api/v1/dags/cache-test/metrics?limit=10`

### 3. Verify Images

```bash
# Check scheduler-service image
kubectl get pod -n helios -l app=scheduler-service -o "jsonpath={.items[0].spec.containers[0].image}"
# Should output: pratik9634/scheduler-service:cache

# Check frontend image
kubectl get pod -n helios -l app=frontend -o "jsonpath={.items[0].spec.containers[0].image}"
# Should output: pratik9634/frontend:cache
```

## Troubleshooting

### Images Not Updating

If Kubernetes is still using old images:

```bash
# Force pod recreation
kubectl rollout restart deployment/scheduler-service -n helios
kubectl rollout restart deployment/frontend -n helios

# Or delete pods to force re-pull
kubectl delete pod -n helios -l app=scheduler-service
kubectl delete pod -n helios -l app=frontend
```

### 403 Errors on POST /api/v1/dags

1. Check scheduler-service logs:
   ```bash
   kubectl logs -n helios -l app=scheduler-service --tail=100
   ```

2. Test direct API access:
   ```bash
   kubectl port-forward -n helios svc/scheduler-service 8080:8080
   curl -X POST http://localhost:8080/api/v1/dags \
     -H "Content-Type: application/json" \
     -d '{"apiVersion":"v1","dagName":"test","tasks":[]}'
   ```

3. Check CORS configuration in CorsConfig.java
4. Verify Nginx proxy configuration in frontend

### Cache Not Working

1. Check DaemonSet is running on all nodes:
   ```bash
   kubectl get daemonset -n helios helios-cache-prep
   kubectl get pods -n helios -l app=helios-cache-prep
   ```

2. Check cache directory exists on nodes:
   ```bash
   kubectl exec -n helios -it <cache-prep-pod-name> -- ls -la /host/var/helios/cache
   ```

3. Check init container logs:
   ```bash
   kubectl logs -n helios <job-pod-name> -c smart-downloader
   ```

4. Check job has cache volume mounted:
   ```bash
   kubectl get job -n helios <job-name> -o yaml | grep -A 10 volumes
   ```

## Version Tags

- `healthfix`: Previous version with health endpoint fixes
- `cache`: Current version with Phase 2 caching and metrics (this version)

## Notes

- The cache is node-local, so artifacts are only reused when tasks run on the same node
- Cache directory is at `/var/helios/cache` on each node
- Metrics are stored in Redis with a rolling window of last 100 runs per DAG
- The DaemonSet needs privileged access to create host directories
