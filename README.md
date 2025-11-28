# Dag-Executing-cloud-platform

## Build and Deploy Scheduler Service

This section provides the full sequence of commands to build, push, and deploy the scheduler-service with the init container shell syntax fix.

### Prerequisites

- Docker installed and authenticated to Docker Hub
- kubectl configured with access to your Kubernetes cluster
- Maven or use the included Maven wrapper

### Step 1: Build the Application

```bash
cd scheduler-service

# Build the JAR file (skip tests for faster build)
./mvnw clean package -DskipTests

# Or run tests as well
./mvnw clean package
```

### Step 2: Build and Push Docker Image

```bash
cd scheduler-service

# Build the Docker image
docker build -t pratik9634/scheduler-service:initfix .

# Push to Docker Hub
docker push pratik9634/scheduler-service:initfix
```

### Step 3: Apply Kubernetes Manifests

```bash
# Apply all Kubernetes manifests in order
kubectl apply -f k8s/00-namespace.yaml
kubectl apply -f k8s/01-redis.yaml
kubectl apply -f k8s/02-rabbitmq.yaml
kubectl apply -f k8s/03-scheduler-service.yaml
kubectl apply -f k8s/04-frontend.yaml
kubectl apply -f k8s/05-scheduler-rbac.yaml
kubectl apply -f k8s/06-minio.yaml
```

### Step 4: Restart the Deployment

```bash
# Restart the scheduler-service deployment to pick up the new image
kubectl rollout restart deployment/scheduler-service -n helios

# Wait for the rollout to complete
kubectl rollout status deployment/scheduler-service -n helios

# Verify the pods are running
kubectl get pods -n helios -l app=scheduler-service
```

### All-in-One Script

For convenience, here's a complete script to build, push, and deploy:

```bash
#!/bin/bash
set -e

echo "=== Building scheduler-service JAR ==="
cd scheduler-service
./mvnw clean package -DskipTests
cd ..

echo "=== Building Docker image ==="
docker build -t pratik9634/scheduler-service:initfix scheduler-service/

echo "=== Pushing Docker image ==="
docker push pratik9634/scheduler-service:initfix

echo "=== Applying Kubernetes manifests ==="
kubectl apply -f k8s/

echo "=== Restarting scheduler-service deployment ==="
kubectl rollout restart deployment/scheduler-service -n helios
kubectl rollout status deployment/scheduler-service -n helios

echo "=== Deployment complete! ==="
kubectl get pods -n helios -l app=scheduler-service
```

### Fix Details

The init container script was updated to version 3.0 to handle the case when `HELIOS_INPUT_ARTIFACTS` is empty. The fix wraps the artifact processing loop in a conditional check:

```shell
if [ -n "$HELIOS_INPUT_ARTIFACTS" ]; then
  # Process artifacts...
fi
```

This prevents the shell syntax error: `unexpected redirection` that occurred when `HELIOS_INPUT_ARTIFACTS` was empty, causing the `echo` command to pipe empty content to the `while` loop.