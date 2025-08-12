# Event Pipeline & Secure Signal Dashboard Backend

**Objective**: Kafka-backed event processing pipeline for high-frequency signals with real-time anomaly detection, idempotent storage, and comprehensive observability.

## 🏗️ Architecture

```
Signal Emitter ──▶ Kafka (signal-events) ──▶ Signal Processor ──▶ PostgreSQL
      │                     │                        │
      │ /emit               │                        └──▶ Prometheus (metrics)
      ▼                     ▼
  REST Endpoint      DLQ (signal-events-dlq)
                            │
                            ▼
                     Anomaly Detector ──▶ Kafka (anomaly-events)
                            │
                            └──▶ PostgreSQL (anomalies)

Prometheus scrapes all services → Grafana dashboard + alerts
```

## ✅ Implementation Status

### **Core Pipeline**
- ✅ **Signal Emitter**: Produces 1k signals/sec to `signal-events` (6 partitions)
- ✅ **Signal Processor**: Consumes with idempotent upserts (`ON CONFLICT DO NOTHING`)
- ✅ **Anomaly Detector**: Rule-based detection → `anomaly-events` (3 partitions)
- ✅ **Dead Letter Queue**: `signal-events-dlq` for invalid messages

### **Data Schema** (Per Specification)
```sql
-- signals table
CREATE TABLE signals (
    id UUID PRIMARY KEY,                    -- event_id (UUIDv1)
    user_id TEXT,                          -- "user_123"
    source TEXT,                           -- "web", "mobile", "api"
    type TEXT,                             -- "login", "purchase", "click", etc.
    event_ts TIMESTAMPTZ,                  -- event timestamp
    ingest_ts TIMESTAMPTZ,                 -- ingestion timestamp
    payload JSONB                          -- {"session_id": "sess_123", "data": "sample_data_42"}
);

-- anomalies table  
CREATE TABLE anomalies (
    id UUID PRIMARY KEY,                    -- anomaly UUID
    user_id TEXT,                          -- from original signal
    anomaly_type TEXT,                     -- "high_data_value", "suspicious_session", etc.
    severity INT,                          -- 1 (simplified from arbitrary rankings)
    detection_ts TIMESTAMPTZ,             -- when anomaly was detected
    signal_event_id UUID REFERENCES signals(id),  -- FK to original signal
    context JSONB                          -- {"original_signal": {...}}
);
```

### **Kafka Configuration**
- ✅ **Topics**: `signal-events` (6 partitions), `anomaly-events` (3 partitions), `signal-events-dlq` (1 partition)
- ✅ **Producer**: `acks=all`, `retries=5`
- ✅ **Consumer**: `enable_auto_commit=false`, commit after DB write

### **Observability & Metrics**
- ✅ **Prometheus** metrics on ports 8011-8013
- ✅ **Grafana** dashboard with throughput, latency, consumer lag
- ✅ **Alert rules** for high lag, DLQ rate, DB failures

## 🚀 Quick Start

```bash
# One-command startup
docker compose up -d --build

# Verify all services healthy
make health

# View real-time stats
make db-stats
```

## 🧪 Testing & Validation

### **Manual Testing**
```bash
# Manual signal emission (NEW!)
curl -X POST http://localhost:8001/emit

# Health checks
curl http://localhost:8001/health  # Signal Emitter
curl http://localhost:8002/health  # Signal Processor  
curl http://localhost:8003/health  # Anomaly Detector

# Real-time metrics
curl http://localhost:8011/metrics | grep signals_produced_total
curl http://localhost:8012/metrics | grep signals_consumed_total  
curl http://localhost:8013/metrics | grep anomalies_detected_total
```

### **Database Inspection**
```bash
# Signal counts
docker compose exec postgres psql -U postgres -d eventpipeline -c "SELECT COUNT(*) FROM signals;"

# Anomaly counts  
docker compose exec postgres psql -U postgres -d eventpipeline -c "SELECT COUNT(*) FROM anomalies;"

# Recent signals
docker compose exec postgres psql -U postgres -d eventpipeline -c "SELECT user_id, type, event_ts FROM signals ORDER BY event_ts DESC LIMIT 5;"
```

### **Kafka Topic Inspection**
```bash
# List topics
make kafka-topics

# View signal events
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic signal-events --from-beginning --max-messages 3

# View anomaly events  
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic anomaly-events --from-beginning --max-messages 3

# Check DLQ
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic signal-events-dlq --from-beginning --max-messages 2
```

## 📋 Acceptance Tests (All Passed ✅)

### **1. Stack Startup**
```bash
docker compose up -d --build  # ✅ Runs without errors
```

### **2. Load Test (1k Events)**
```bash
make load  # ✅ Postgres row count increases ~1k
```

### **3. Idempotency Test**
```bash
# Send same signal multiple times → DB count increases by 1 only
# ✅ Implemented via ON CONFLICT (id) DO NOTHING
```

### **4. DLQ Test**
```bash
# Send invalid messages → DLQ counter increments
# ✅ signals_dlq_total metric increases correctly
```

### **5. Consumer Lag Test**
```bash
curl -s http://localhost:8012/metrics | grep consumer_lag
# ✅ Stays at 0.0 (threshold: 1000)
```

## 🔧 Developer Experience

### **Makefile Commands**
```bash
make up           # Start entire stack
make down         # Stop stack  
make clean        # Stop + clean volumes
make load         # Load test (1k events)
make logs         # View all logs
make logs-<service>  # Specific service logs
make health       # Health check all services
make db-stats     # Database statistics
make kafka-topics # List Kafka topics
make status       # Docker compose status
```

### **Bruno API Collection**
Located in `bruno-collection/`:
- ✅ Health checks for all services
- ✅ Prometheus metrics endpoints
- ✅ **NEW**: `/emit` endpoint for manual testing

Import into Bruno API client for interactive testing.

## 📊 Live Metrics

### **Production Metrics** (Prometheus)
```bash
# Producer (Signal Emitter - :8011)
signals_produced_total           # Total signals produced
produce_errors_total             # Production errors  
produce_latency_seconds          # Production latency histogram

# Processor (Signal Processor - :8012)  
signals_consumed_total           # Total signals consumed
db_write_failures_total          # Database write failures
processing_latency_seconds       # Processing latency histogram
consumer_lag                     # Consumer lag gauge
signals_dlq_total               # DLQ messages counter

# Detector (Anomaly Detector - :8013)
anomalies_detected_total{type,severity}  # Anomalies by type/severity
detection_latency_seconds                # Detection latency histogram
```


## 🔗 Access URLs

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
- **Signal Emitter**: http://localhost:8001 (health + /emit)
- **Signal Processor**: http://localhost:8002 (health)
- **Anomaly Detector**: http://localhost:8003 (health)

## Configuration (Env Vars)
- `KAFKA_BOOTSTRAP_SERVERS` (default: `kafka:29092`)
- `DATABASE_URL` (default: `postgresql://postgres:postgres@postgres:5432/eventpipeline`)
- `SIGNALS_PER_SECOND` (default: `1000`)
- `BATCH_SIZE` (default: `100`)

## Developer Commands
```bash
make up      # Start stack
make down    # Stop stack
make logs    # View logs
make load    # Load test
make health  # Health checks
```

## Data Model (Minimal)
- `signals(id, user_id, type, timestamp, payload, ...)`
- `anomalies(id, user_id, anomaly_type, severity, detection_timestamp, ...)`

## Notes
- Local stack includes Kafka (KRaft, no ZK), Postgres, Prometheus; Grafana is optional but provisioned in compose.
