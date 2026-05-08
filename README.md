# CS523 Big Data Technologies - Final Project

IoT Data Processing Pipeline using Kafka, Spark, and Hadoop

## Overview

This project implements a real-time IoT data processing pipeline with:

- **Producer**: Fetches IoT sensor data from `api.sensor.community` and streams to Kafka
- **Consumer**: Reads messages from Kafka topic for further processing
- **Message Queue**: Apache Kafka with Zookeeper
- **Message Size**: Supports up to 20MB messages for bulk IoT data

## Architecture

```
IoT Sensor API → Producer → Kafka Topic (iot-data) → Consumer
                  ↓
            Zookeeper (coordination)
```

## Prerequisites

- Docker and Docker Compose installed
- Minimum 4GB available memory

## Quick Start

### Build and Run All Services

```bash
docker-compose up --build -d
```

### Check Service Status

```bash
# View all running containers
docker-compose ps

# View producer logs
docker-compose logs -f producer

# View consumer logs
docker-compose logs -f consumer

# View kafka logs
docker-compose logs -f kafka
```

### Stop All Services

```bash
docker-compose down
```

## Services

### Zookeeper (Port: 2181)

- Manages Kafka broker coordination
- Stores cluster metadata

### Kafka Broker (Port: 9092)

- Topic: `iot-data`
- Max message size: 20MB
- Replication factor: 1

### Producer

- Fetches IoT sensor data every 5 minutes
- Sends data to Kafka with timestamp-based partitioning
- Logs to Docker logs via logging module

### Consumer

- Listens to `iot-data` topic
- Processes messages in real-time
- Logs offset and data to Docker logs

## Configuration

### Kafka Topic Settings

- **Topic Name**: `iot-data`
- **Message Size Limit**: 20MB (configured for bulk data, but currently sends one by one)
- **Auto Offset Reset**: earliest

### Producer Configuration

- **API Source**: https://api.sensor.community/static/v2/data.json
- **Sync Interval**: 5 minutes (300 seconds)
- **Partition Key**: Current timestamp (milliseconds)

## Troubleshooting

### Producer/Consumer won't start

1. Ensure Kafka and Zookeeper are healthy: `docker-compose ps`
2. Check healthchecks: `docker-compose logs kafka`
3. Restart specific service: `docker-compose restart producer`

### MessageSizeTooLargeError

The message size has been configured to 20MB in both:

- `docker-compose.yml` (Kafka broker settings)
- `producer/job.py` (KafkaProducer client)

### Port Conflicts

If ports are in use, modify the port mappings in `docker-compose.yml`:

- Zookeeper: 2181
- Kafka: 9092
- Postgres: 5432

## Development

All Python services use logging instead of print for proper Docker log capture.

### File Structure

```
.
├── producer/           # Kafka producer service
│   ├── job.py         # IoT data fetching job
│   ├── app.py         # Flask app (optional)
│   ├── Dockerfile
│   └── requirements.txt
├── consumer/          # Kafka consumer service
│   ├── consumer.py
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
└── README.md
```

## Notes

- Services have healthchecks configured to ensure proper startup order
- All logging goes to stdout (captured by Docker logs)
- The consumer reads all historical messages due to `auto_offset_reset: earliest`
