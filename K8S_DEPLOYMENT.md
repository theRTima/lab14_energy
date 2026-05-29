# Kubernetes Deployment Guide

## Prerequisites

You need either **minikube** or **k3s** installed.

### Option 1: Install Minikube (Recommended for development)

**macOS:**
```bash
brew install minikube
minikube start --cpus=4 --memory=4096
```

**Linux:**
```bash
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
minikube start --cpus=4 --memory=4096
```

### Option 2: Install k3s (Lightweight Kubernetes)

**Linux only:**
```bash
curl -sfL https://get.k3s.io | sh -
sudo chmod 644 /etc/rancher/k3s/k3s.yaml
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
```

## Deploy the System

Once minikube or k3s is running:

```bash
./deploy-k8s.sh
```

## Monitor the System

Watch HPA scaling:
```bash
watch kubectl get hpa -n energy-monitoring
```

View pods:
```bash
kubectl get pods -n energy-monitoring
```

View collector logs:
```bash
kubectl logs -f -l app=collector -n energy-monitoring
```

View Python client logs:
```bash
kubectl logs -f -l app=python-client -n energy-monitoring
```

## Test HPA Scaling

Generate load to trigger autoscaling:
```bash
# Increase the number of meters to create more CPU load
kubectl set env deployment/collector TOTAL_METERS=5000 -n energy-monitoring
```

Watch the HPA scale up collectors automatically when CPU exceeds 70%.

## Cleanup

```bash
./cleanup-k8s.sh
```

To stop minikube:
```bash
minikube stop
```

To uninstall k3s:
```bash
sudo /usr/local/bin/k3s-uninstall.sh
```

## Architecture in Kubernetes

- **etcd**: StatefulSet with persistent volume (1 replica)
- **flight-server**: Deployment (1 replica) - receives Arrow data
- **collector**: Deployment (2-8 replicas) - auto-scales based on CPU/memory
- **analyzer**: Deployment (1 replica) - analyzes etcd data
- **python-client**: Deployment (1 replica) - fetches from Flight server

## HPA Behavior

- **Scale Up**: When CPU > 70% or Memory > 80%
  - Adds 100% more pods or 2 pods (whichever is higher) every 30s
  - Max 8 replicas
  
- **Scale Down**: When CPU < 70% and Memory < 80%
  - Removes 50% of pods every 60s
  - Waits 5 minutes before scaling down (stabilization)
  - Min 2 replicas
