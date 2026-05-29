# Energy Consumption Analysis System

Рощин Тимур Максимович 220032-11, Лабораторная Номер 14, вариант 19, задания повышенной сложности

A distributed energy consumption monitoring system with parallel Go collectors, Kafka streaming, real-time Python analytics, and a live Streamlit dashboard.

## Architecture

- **Meter Emulator**: Generates realistic electricity meter readings with time-of-day patterns
- **Collectors**: Multiple parallel Go instances that collect data from assigned meter shards
- **Coordinator**: Uses etcd for distributed lock management and shard assignment
- **Kafka**: Streaming transport for aggregated data from collectors to consumers
- **Analyzer (Go)**: Aggregates and analyzes collected energy consumption data
- **Kafka Analyzer (Python)**: Real-time consumer that reads from Kafka and prints statistics
- **Dashboard (Streamlit)**: Live web UI with charts and metrics, consuming from Kafka
- **Flight Server**: Backward-compatible Arrow Flight data server

## Prerequisites

- Go 1.22+
- Docker and Docker Compose
- Python 3.12+ (for local dev)
- Minikube or k3s (for K8s deployment)

## Quick Start (Docker Compose)

```bash
docker compose build
docker compose up -d
```

This starts all services:
- etcd, zookeeper, kafka
- 4 Go collectors + flight-server
- Go analyzer + Python kafka-analyzer
- Python flight client
- Streamlit dashboard

### Access the Dashboard

Once everything is running:

```bash
open http://localhost:8501
```

The dashboard shows live charts updating every 2 seconds:
- Real-time metrics (readings/sec, total energy, active meters)
- Time-series chart of energy over time
- Per-collector breakdown
- Meter power distribution histogram

### Stop the System

```bash
docker compose down
```

To also remove volumes (wipes etcd and kafka state):

```bash
docker compose down -v
```

## Quick Start (Kubernetes)

See [K8S_DEPLOYMENT.md](K8S_DEPLOYMENT.md).

```bash
./deploy-k8s.sh
```

### Access the Dashboard (K8s)

```bash
kubectl port-forward -n energy-monitoring service/dashboard 8501:8501
```

Then visit http://localhost:8501.

Or if using NodePort (port 30851):

```bash
open http://localhost:30851
```

### Stop K8s

```bash
./cleanup-k8s.sh
```

## Services Overview

| Service | Port | Description |
|---------|------|-------------|
| etcd | 2379 | Distributed coordination |
| zookeeper | 2181 | Kafka coordination |
| kafka | 9092 | Message broker |
| flight-server | 8815 | Arrow Flight data server |
| dashboard | 8501 | Streamlit live dashboard |
| python-client | 5000 | Flight client |

## Pipeline

```
Meter Emulator → Collectors (Go) → Kafka
                                      ├── kafka-analyzer (Python) → stdout stats
                                      └── dashboard (Streamlit)   → http://localhost:8501
                                    → Flight Server (legacy) → Analyzer
```

## Running Locally (without Docker)

Start etcd:
```bash
docker run -d --name etcd -p 2379:2379 -p 2380:2380 \
  quay.io/coreos/etcd:v3.5.9 \
  /usr/local/bin/etcd \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://localhost:2379
```

Run collectors:
```bash
go run cmd/collector/main.go -id collector-1 -etcd localhost:2379 -meters 1000 -shards 4
go run cmd/collector/main.go -id collector-2 -etcd localhost:2379 -meters 1000 -shards 4
```

Run analyzer:
```bash
go run cmd/analyzer/main.go -etcd localhost:2379 -interval 30s -window 5m
```

## Configuration

### Collector Flags
- `-id`: Unique collector identifier
- `-etcd`: etcd endpoint (default: localhost:2379)
- `-meters`: Total number of meters (default: 1000)
- `-shards`: Number of shards to divide meters into (default: 4)
- `-interval`: Collection interval (default: 5s)
- `-kafka-brokers`: Kafka brokers (comma-separated)
- `-kafka-topic`: Kafka topic for aggregated data

### Analyzer Flags
- `-etcd`: etcd endpoint (default: localhost:2379)
- `-interval`: Analysis interval (default: 30s)
- `-window`: Time window for analysis (default: 5m)

## Benchmark Results

Go is roughly **52–93× faster** than equivalent Python async collector in throughput (readings/sec). See `bench_report/benchmark_report.md` for details.

## Project Structure

```
.
├── cmd/
│   ├── collector/       # Collector service
│   ├── analyzer/        # Analyzer service
│   └── benchmark/       # Go benchmark binary
├── pkg/
│   ├── meter/           # Meter emulation
│   ├── coordinator/     # etcd-based coordination
│   ├── aggregator/      # Tumbling window aggregation
│   ├── sender/          # Flight + Kafka senders
│   └── flightserver/    # Arrow Flight server
├── python/
│   ├── async_collector.py
│   ├── kafka_analyzer.py
│   ├── dashboard.py
│   ├── bench_compare.py
│   └── requirements.txt
├── k8s/                 # Kubernetes manifests
├── docker-compose.yml
├── Dockerfile.collector
├── Dockerfile.analyzer
├── Dockerfile.kafka-analyzer
├── Dockerfile.dashboard
├── Dockerfile.python
├── deploy-k8s.sh
├── cleanup-k8s.sh
└── K8S_DEPLOYMENT.md
```
