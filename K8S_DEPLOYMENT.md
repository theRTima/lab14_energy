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

This deploys all services into the `energy-monitoring` namespace.

### Access the Dashboard

**Option 1 — Port forwarding (recommended):**
```bash
kubectl port-forward -n energy-monitoring service/dashboard 8501:8501
```
Then open http://localhost:8501.

**Option 2 — NodePort (minikube only):**
```bash
minikube service dashboard -n energy-monitoring
```

## Monitor the System

View all pods:
```bash
kubectl get pods -n energy-monitoring
```

Watch HPA scaling:
```bash
watch kubectl get hpa -n energy-monitoring
```

View collector logs:
```bash
kubectl logs -f -l app=collector -n energy-monitoring
```

View kafka-analyzer logs:
```bash
kubectl logs -f -l app=kafka-analyzer -n energy-monitoring
```

View dashboard logs:
```bash
kubectl logs -f -l app=dashboard -n energy-monitoring
```

View Python client logs:
```bash
kubectl logs -f -l app=python-client -n energy-monitoring
```

## Stop the System

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

- **etcd**: StatefulSet (1 replica) — distributed coordination
- **zookeeper**: StatefulSet (1 replica) — Kafka coordination
- **kafka**: StatefulSet (1 replica) — message broker
- **flight-server**: Deployment (1 replica) — receives Arrow data (legacy)
- **collector**: Deployment (2–8 replicas) — auto-scales via HPA
- **analyzer**: Deployment (1 replica) — analyzes etcd data
- **kafka-analyzer**: Deployment (1 replica) — real-time Kafka consumer
- **dashboard**: Deployment (1 replica) — Streamlit UI on port 8501 (NodePort 30851)
- **python-client**: Deployment (1 replica) — fetches from Flight server

## Pipeline (K8s)

```
Meters → Collectors (Go) → Kafka
                             ├── kafka-analyzer (Python) → logs
                             └── dashboard (Streamlit)   → port 8501
                           → Flight Server (legacy) → Analyzer
```

## Test HPA Scaling

Generate load to trigger autoscaling:
```bash
kubectl set env deployment/collector TOTAL_METERS=5000 -n energy-monitoring
```

Watch the HPA scale up collectors automatically when CPU exceeds 70%.

## HPA Behavior

- **Scale Up**: When CPU > 70% or Memory > 80%
  - Adds 100% more pods or 2 pods (whichever is higher) every 30s
  - Max 8 replicas
  
- **Scale Down**: When CPU < 70% and Memory < 80%
  - Removes 50% of pods every 60s
  - Waits 5 minutes before scaling down (stabilization)
  - Min 2 replicas
