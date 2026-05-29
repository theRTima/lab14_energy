# Energy Consumption Analysis System

Рощин Тимур Максимович, Лабораторная Номер 14, вариант 19, задания повышенной сложности

A distributed energy consumption monitoring system with parallel Go collectors coordinated by etcd.

## Architecture

- **Meter Emulator**: Generates realistic electricity meter readings with time-of-day patterns
- **Collectors**: Multiple parallel Go instances that collect data from assigned meter shards
- **Coordinator**: Uses etcd for distributed lock management and shard assignment
- **Storage**: Stores readings in etcd with time-series organization
- **Analyzer**: Aggregates and analyzes collected energy consumption data

## Features

- Distributed shard-based data collection
- Automatic shard assignment via etcd distributed locks
- Emulated electricity meters with realistic consumption patterns
- Real-time energy consumption analysis
- Horizontal scalability (add more collectors as needed)

## Prerequisites

- Go 1.22+
- Docker and Docker Compose

## Quick Start

1. Install dependencies:
```bash
go mod download
```

2. Start the system with Docker Compose:
```bash
docker-compose up --build
```

This will start:
- 1 etcd instance
- 4 collector instances (each handling a shard of 1000 meters)
- 1 analyzer instance

3. Watch the logs to see collectors acquiring shards and the analyzer producing statistics.

## Running Locally

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

### Analyzer Flags
- `-etcd`: etcd endpoint (default: localhost:2379)
- `-interval`: Analysis interval (default: 30s)
- `-window`: Time window for analysis (default: 5m)

## How It Works

1. **Shard Assignment**: When a collector starts, it requests a shard from the coordinator
2. **Distributed Locking**: The coordinator uses etcd distributed locks to ensure each shard is assigned to only one collector
3. **Data Collection**: Each collector generates readings from its assigned meters and stores them in etcd
4. **Analysis**: The analyzer periodically retrieves all readings from the time window and computes statistics

## Project Structure

```
.
├── cmd/
│   ├── collector/    # Collector service
│   └── analyzer/     # Analyzer service
├── pkg/
│   ├── meter/        # Meter emulation
│   ├── coordinator/  # etcd-based coordination
│   └── storage/      # Data storage layer
├── docker-compose.yml
├── Dockerfile.collector
└── Dockerfile.analyzer
```
