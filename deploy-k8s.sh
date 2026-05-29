#!/bin/bash
set -e

echo "=== Energy Monitoring System - Kubernetes Deployment ==="
echo ""

# Detect cluster type
if command -v minikube &> /dev/null && minikube status &> /dev/null; then
    CLUSTER_TYPE="minikube"
    echo "Detected: Minikube"
elif command -v k3s &> /dev/null; then
    CLUSTER_TYPE="k3s"
    echo "Detected: k3s"
else
    echo "Error: Neither minikube nor k3s detected"
    exit 1
fi

echo ""
echo "Step 1: Building Docker images..."
docker-compose build

if [ "$CLUSTER_TYPE" = "minikube" ]; then
    echo ""
    echo "Step 2: Loading images into Minikube..."
    eval $(minikube docker-env)
    docker-compose build

    echo ""
    echo "Step 3: Enabling metrics-server..."
    minikube addons enable metrics-server
else
    echo ""
    echo "Step 2: Importing images to k3s..."
    docker save lab14_energy-collector:latest | sudo k3s ctr images import -
    docker save lab14_energy-analyzer:latest | sudo k3s ctr images import -
    docker save lab14_energy-python-client:latest | sudo k3s ctr images import -
    docker save lab14_energy-kafka-analyzer:latest | sudo k3s ctr images import -
    docker save lab14_energy-dashboard:latest | sudo k3s ctr images import -

    echo ""
    echo "Step 3: Checking metrics-server..."
    if ! kubectl get deployment metrics-server -n kube-system &> /dev/null; then
        echo "Installing metrics-server..."
        kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml
        kubectl patch deployment metrics-server -n kube-system --type='json' -p='[{"op": "add", "path": "/spec/template/spec/containers/0/args/-", "value": "--kubelet-insecure-tls"}]'
    fi
fi

echo ""
echo "Step 4: Deploying to Kubernetes..."
kubectl apply -f k8s/

echo ""
echo "Step 5: Waiting for deployments to be ready..."
kubectl wait --for=condition=ready pod -l app=etcd -n energy-monitoring --timeout=120s
kubectl wait --for=condition=ready pod -l app=flight-server -n energy-monitoring --timeout=120s
kubectl wait --for=condition=ready pod -l app=collector -n energy-monitoring --timeout=120s
kubectl wait --for=condition=ready pod -l app=kafka -n energy-monitoring --timeout=120s
kubectl wait --for=condition=ready pod -l app=kafka-analyzer -n energy-monitoring --timeout=120s
kubectl wait --for=condition=ready pod -l app=dashboard -n energy-monitoring --timeout=120s

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Useful commands:"
echo "  kubectl get pods -n energy-monitoring"
echo "  kubectl get hpa -n energy-monitoring"
echo "  kubectl logs -f -l app=collector -n energy-monitoring"
echo "  kubectl logs -f -l app=python-client -n energy-monitoring"
echo "  kubectl logs -f -l app=kafka-analyzer -n energy-monitoring"
echo "  kubectl logs -f -l app=dashboard -n energy-monitoring"
echo ""
echo "Dashboard URL:"
echo "  Minikube: minikube service dashboard -n energy-monitoring"
echo "  k3s: kubectl port-forward service/dashboard -n energy-monitoring 8501:8501"
echo ""
echo "To watch HPA scaling:"
echo "  watch kubectl get hpa -n energy-monitoring"
