#!/bin/bash
set -e

# Build and push Docker images for Phase 2 caching
# Usage: ./build-and-push.sh [tag]

TAG=${1:-cache}
DOCKER_USER=${DOCKER_USER:-pratik9634}

echo "Building images with tag: $TAG"
echo "Docker user: $DOCKER_USER"
echo ""

# Build scheduler-service
echo "==> Building scheduler-service..."
cd scheduler-service
mvn clean package -DskipTests
docker build -t $DOCKER_USER/scheduler-service:$TAG .
echo "==> Pushing scheduler-service:$TAG..."
docker push $DOCKER_USER/scheduler-service:$TAG
cd ..

# Build frontend
echo "==> Building frontend..."
cd dag-ui-futuristic
npm install
npm run build
docker build -t $DOCKER_USER/frontend:$TAG .
echo "==> Pushing frontend:$TAG..."
docker push $DOCKER_USER/frontend:$TAG
cd ..

echo ""
echo "==> Build and push complete!"
echo "Images:"
echo "  - $DOCKER_USER/scheduler-service:$TAG"
echo "  - $DOCKER_USER/frontend:$TAG"
echo ""
echo "To deploy, run:"
echo "  kubectl apply -f k8s/07-cache-prep-daemonset.yaml"
echo "  kubectl apply -f k8s/03-scheduler-service.yaml"
echo "  kubectl apply -f k8s/04-frontend.yaml"
