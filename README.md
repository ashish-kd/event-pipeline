# Event Pipeline & Dashboard Backend Upgrade (Minimal)

**Objective**: Implement a Kafka-backed event processing pipeline for the Secure Signal Dashboard to handle high-frequency signals and enable real-time anomaly/event storage with observability.

## Architecture (Minimal)
```
Signal Emitter ──▶ Kafka (signal-events) ──▶ Signal Processor ──▶ PostgreSQL
                           │                         │
                           │                         └──▶ Prometheus (metrics)
                           ▼
                    Anomaly Detector ──▶ Kafka (anomaly-events)
Prometheus scrapes all services; Grafana (optional) visualizes
```

## Scope Implemented
- **Signal Event Stream**: Kafka pipeline ingesting signals `{user_id, type, timestamp, payload}`; async processing → PostgreSQL.
- **Anomaly Detection Stub**: Lightweight rule-based detector; publishes to `anomaly-events` topic.
- **Metrics & Observability**: Prometheus metrics for throughput, anomalies, and latency; optional Grafana dashboard.
- **DevOps**: All services containerized; `docker-compose.yml` with Kafka (KRaft), Postgres, Prometheus, Grafana.
- **Documentation**: This README and diagram.

## Quick Start
```bash
# From repo root
docker compose up -d --build
```

## Access
- Prometheus: `http://localhost:9090`
- Grafana (optional): `http://localhost:3000` (admin/admin)

## Services (Brief)
- `signal-emitter` (FastAPI): produces to `signal-events`
- `signal-processor`: consumes `signal-events`, stores in PostgreSQL
- `anomaly-detector`: rule-based stub, emits to `anomaly-events`
- `prometheus`, `grafana` (optional visualization)


## Metrics Exposed
- **counters**: `signals_produced_total`, `events_processed_total`, `anomalies_detected_total`
- **histograms**: `signal_production_latency_seconds`, `processing_latency_seconds`, `anomaly_detection_latency_seconds`
- **gauges**: `batch_size_current`, `active_user_sessions`

## Configuration (Env Vars)
- `KAFKA_BOOTSTRAP_SERVERS` (default: `kafka:29092`)
- `DATABASE_URL` (default: Postgres in compose)
- `BATCH_SIZE` (processor), `SIGNALS_PER_SECOND`, `BURST_MODE` (emitter)

## Data Model (Minimal)
- `signals(id, user_id, type, timestamp, payload, ...)`
- `anomalies(id, user_id, anomaly_type, severity, detection_timestamp, ...)`

## Notes
- Local stack includes Kafka (KRaft, no ZK), Postgres, Prometheus; Grafana is optional but provisioned in compose.
