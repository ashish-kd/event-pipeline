# Event Processing Pipeline

A high-performance, Kafka-backed event processing pipeline built with Python microservices. This system can handle 1000+ events per second with real-time anomaly detection, comprehensive monitoring, and REST API access.

## 🏗️ Architecture

```
┌─────────────────┐    ┌──────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Signal        │    │    Kafka     │    │   Signal        │    │   PostgreSQL    │
│   Emitter       ├───▶│ (KRaft mode) │───▶│   Processor     ├───▶│   Database      │
│   (FastAPI)     │    │ (no ZK)      │    │   (Consumer)    │    │   (Storage)     │
└─────────────────┘    └──────┬───────┘    └─────────────────┘    └─────────────────┘
                              │                       │                       │
                              │                       ▼                       │
                              │              ┌─────────────────┐              │
                              │              │   Anomaly       │              │
                              │              │   Detector      │              │
                              │              │ (Concurrent)    │              │
                              │              └─────────────────┘              │
                              │                       │                       │
                              ▼                       ▼                       ▼
                    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
                    │   REST API      │    │   Prometheus    │    │   Grafana       │
                    │   Service       │    │   (scrapes)     │    │   (dashboards)  │
                    │   (Query)       │    └─────────────────┘    └─────────────────┘
                    └─────────────────┘
```

### Components

1. **Signal Emitter** - Generates high-frequency signals (1000+ events/second)
2. **Signal Processor** - Kafka consumer with batch processing and PostgreSQL storage
3. **Anomaly Detector** - Rule-based anomaly detection with real-time processing
4. **REST API** - Query interface for signals, anomalies, and metrics
5. **Monitoring** - Prometheus scrapes service metrics; Grafana visualizes
6. **Database** - PostgreSQL with optimized schema and indexing

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- 8GB+ RAM recommended for full stack
- Ports 3000, 5432, 8000-8003, 9090, 9092 available

### Setup and Run

```bash
# Clone the repository
git clone <repository-url>
cd event-pipeline

# First run (or after Kafka config changes): clean volumes for a fresh KRaft cluster
docker compose down -v

# Build and start the entire stack
docker compose up -d --build

# Check services status
docker compose ps

# View logs
docker compose logs -f kafka | cat
docker compose logs -f signal-emitter | cat
```

### Access Points

- **Grafana Dashboard**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **REST API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **Metrics (host ports)**:
  - Signal Emitter: http://localhost:8011/metrics
  - Signal Processor: http://localhost:8012/metrics
  - Anomaly Detector: http://localhost:8013/metrics
  - API Service: http://localhost:8010/metrics

## 📊 Services Overview

### Signal Emitter Service
- **Port**: 8000 (API), 8001 (Metrics)
- **Purpose**: Generate realistic user behavior signals
- **Rate**: Configurable (default 1000 events/sec)
- **Signal Types**: login, purchase, click, api_call, file_access

### Signal Processor Service
- **Port**: 8000 (API), 8001 (Metrics)
- **Purpose**: Consume and store signals in PostgreSQL
- **Features**: Batch processing, concurrent chunked inserts, dead letter queue, connection pooling
- **Batch Size**: Configurable (default 100)

### Anomaly Detector Service
- **Port**: 8000 (API), 8001 (Metrics)
- **Purpose**: Real-time anomaly detection
- **Concurrency**: Concurrent detection + store/publish workers
- **Rules**: 
  - High login frequency (>10/minute)
  - Large purchases (>$1000)
  - API failure patterns (>5 failures/minute)
  - Unusual time activity (3AM patterns)

### REST API Service
- **Port**: 8000 (API), 8001 (Metrics)
- **Purpose**: Query interface for signals and anomalies
- **Features**: Pagination, filtering, comprehensive metrics

## 🔧 Configuration

### Environment Variables

| Service | Variable | Default | Description |
|---------|----------|---------|-------------|
| Signal Emitter | `SIGNALS_PER_SECOND` | 1000 | Events per second to generate |
| Signal Emitter | `BURST_MODE` | false | Enable burst traffic simulation |
| Signal Processor | `BATCH_SIZE` | 100 | Batch size for processing |
| Signal Emitter | `EMITTER_WORKERS` | 4 | Number of concurrent Kafka send workers |
| Signal Emitter | `EMITTER_QUEUE_MAXSIZE` | 10000 | Max in-memory queue size |
| Signal Emitter | `EMITTER_FLUSH_EVERY` | 1000 | Flush Kafka producer after N sends |
| All Services | `KAFKA_BOOTSTRAP_SERVERS` | kafka:29092 | Kafka connection |
| All Services | `DATABASE_URL` | postgres://... | PostgreSQL connection |

### Scaling Configuration

```yaml
# docker-compose.override.yml
version: '3.8'
services:
  signal-emitter:
    environment:
      SIGNALS_PER_SECOND: 2000
      BURST_MODE: true
  
  signal-processor:
    environment:
      BATCH_SIZE: 200
    deploy:
      replicas: 2
```

## 📡 API Documentation

### Signals Endpoint

```bash
# Get recent signals
curl "http://localhost:8000/signals?page=1&size=50"

# Filter by user and signal type
curl "http://localhost:8000/signals?user_id=123&signal_type=login"

# Time-based filtering (timestamps in milliseconds)
curl "http://localhost:8000/signals?start_timestamp=1640995200000"
```

### Anomalies Endpoint

```bash
# Get recent anomalies
curl "http://localhost:8000/anomalies?page=1&size=50"

# Filter by severity
curl "http://localhost:8000/anomalies?severity=high"

# Get user-specific anomalies
curl "http://localhost:8000/users/123/anomalies"
```

### Metrics Endpoint

```bash
# Get system metrics for last 24 hours
curl "http://localhost:8000/metrics"

# Get metrics for custom time range
curl "http://localhost:8000/metrics?hours=168"  # Last week
```

### Health Checks

```bash
# Check all services
curl http://localhost:8000/health  # API
curl http://localhost:8001/health  # Signal Emitter
curl http://localhost:8002/health  # Signal Processor
curl http://localhost:8003/health  # Anomaly Detector
```

## 📈 Monitoring Guide

### Grafana Dashboard

1. **Access**: http://localhost:3000 (admin/admin)
2. **Dashboard**: "Event Pipeline Dashboard"
3. **Key Metrics**:
   - Signal production rate by type
   - Processing latency (95th percentile)
   - Anomaly detection rate
   - API request patterns
   - Active user sessions

### Prometheus Metrics

**Signal Emitter Metrics**:
- `signals_produced_total` - Total signals produced by type
- `signal_production_latency_seconds` - Signal generation latency

**Signal Processor Metrics**:
- `events_processed_total` - Total events processed (success/error)
- `processing_latency_seconds` - Batch processing latency
- `batch_size_current` - Current batch size being processed

**Anomaly Detector Metrics**:
- `anomalies_detected_total` - Total anomalies detected by type
- `anomaly_detection_latency_seconds` - Detection processing time
- `active_user_sessions` - Number of active user sessions tracked

**API Metrics**:
- `api_requests_total` - Total API requests by endpoint and status
- `api_request_duration_seconds` - Request processing time

### Alerting Rules Example

```yaml
# prometheus-alerts.yml
groups:
  - name: event-pipeline
    rules:
      - alert: HighAnomalyRate
        expr: rate(anomalies_detected_total[5m]) > 10
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "High anomaly detection rate"
          
      - alert: ProcessingLatencyHigh
        expr: histogram_quantile(0.95, rate(processing_latency_seconds_bucket[5m])) > 5
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Processing latency too high"
```

## 🧪 Load Testing Guide

### Built-in Load Testing

The signal emitter includes built-in load testing capabilities:

```bash
# High load test (2000 events/sec)
docker-compose exec signal-emitter \
  curl -X POST "localhost:8000/configure" \
  -d '{"signals_per_second": 2000, "burst_mode": true}'

# Burst traffic simulation
docker-compose exec signal-emitter \
  curl -X POST "localhost:8000/burst" \
  -d '{"duration_seconds": 60, "multiplier": 5}'
```

### External Load Testing

Using Apache Bench:

```bash
# API load test
ab -n 10000 -c 100 http://localhost:8000/signals

# Sustained load test
ab -n 100000 -c 50 -t 300 http://localhost:8000/metrics
```

Using hey:

```bash
# Install hey
go install github.com/rakyll/hey@latest

# API endpoint testing
hey -n 10000 -c 100 http://localhost:8000/signals
hey -n 5000 -c 50 http://localhost:8000/anomalies
```

### Performance Benchmarks

**Expected Performance** (on 8GB RAM, 4 CPU cores):

- **Signal Generation**: 1000+ events/second sustained
- **Signal Processing**: 800-1200 events/second (batch processing)
- **Anomaly Detection**: 500-800 events/second analyzed
- **API Response Time**: <100ms for most queries
- **Database**: Supports 10K+ concurrent connections

### Load Testing Scenarios

1. **Normal Load**: 1000 events/sec, steady state
2. **Peak Load**: 2000 events/sec with bursts to 5000
3. **Stress Test**: 5000+ events/sec until failure
4. **Endurance Test**: 1000 events/sec for 24+ hours

## 🗄️ Database Schema

### Tables

**signals**
- `id` (BIGSERIAL) - Primary key
- `user_id` (INTEGER) - User identifier
- `type` (VARCHAR) - Signal type
- `timestamp` (BIGINT) - Event timestamp (milliseconds)
- `payload` (JSONB) - Signal data
- `processed_at` (TIMESTAMP) - Processing time
- `created_at` (TIMESTAMP) - Database insert time

**anomalies**
- `id` (BIGSERIAL) - Primary key
- `user_id` (INTEGER) - User identifier
- `anomaly_type` (VARCHAR) - Type of anomaly
- `severity` (VARCHAR) - low/medium/high
- `description` (TEXT) - Human-readable description
- `original_signal_data` (JSONB) - Original signal that triggered anomaly
- `detection_timestamp` (BIGINT) - When anomaly was detected
- `created_at` (TIMESTAMP) - Database insert time

### Indexes

Optimized indexes for high-performance queries:
- User-based queries: `idx_signals_user_id`, `idx_anomalies_user_id`
- Time-based queries: `idx_signals_timestamp`, `idx_anomalies_detection_timestamp`
- Type-based queries: `idx_signals_type`, `idx_anomalies_type`
- Composite indexes for complex queries
- GIN index on JSONB payload fields

### Maintenance

```sql
-- Get user risk score
SELECT * FROM get_user_risk_score(123);

-- Cleanup old data (keep 30 days)
SELECT * FROM cleanup_old_data(30);

-- View recent statistics
SELECT * FROM recent_signal_stats;
SELECT * FROM recent_anomaly_stats;
```

## 🔧 Development

### Local Development Setup

```bash
# Install dependencies for a service
cd signal-emitter
pip install -r requirements.txt

# Run service locally
python app.py

# Run with custom config
SIGNALS_PER_SECOND=500 python app.py
```

### Testing

```bash
# Unit tests
cd signal-emitter
python -m pytest tests/

# Integration tests
cd tests/integration
python test_full_pipeline.py

# Load tests
cd tests/load
python load_test.py
```

### Adding New Anomaly Rules

1. Edit `anomaly-detector/app.py`
2. Add new detection method
3. Update rule processing in `process_signal()`
4. Add new anomaly type to API enums
5. Update Grafana dashboard

Example:
```python
def detect_suspicious_location(self, signal: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Implement new detection logic
    pass
```

## 📝 Signal Types

### Login Signals
```json
{
  "user_id": 123,
  "type": "login",
  "timestamp": 1640995200000,
  "payload": {
    "login_method": "password",
    "success": true,
    "location": "US",
    "ip_address": "192.168.1.100"
  }
}
```

### Purchase Signals
```json
{
  "user_id": 123,
  "type": "purchase",
  "timestamp": 1640995200000,
  "payload": {
    "amount": 99.99,
    "currency": "USD",
    "product_id": "prod_123",
    "payment_method": "credit_card"
  }
}
```

### API Call Signals
```json
{
  "user_id": 123,
  "type": "api_call",
  "timestamp": 1640995200000,
  "payload": {
    "endpoint": "/api/users",
    "method": "GET",
    "status_code": 200,
    "response_time_ms": 45
  }
}
```

## 🚨 Anomaly Types

1. **high_frequency_login** - More than 10 logins per minute
2. **repeated_login_failures** - 3+ consecutive login failures
3. **high_value_purchase** - Purchase amount > $1000
4. **rapid_successive_purchases** - 5+ purchases in one hour
5. **high_api_failure_rate** - 5+ API failures per minute
6. **unusual_time_activity** - Activity during unusual hours (3AM)
7. **unusual_location_login** - Login from new geographic location

## 🛠️ Troubleshooting

### Common Issues

**Kafka Connection Issues**:
```bash
# Check Kafka status
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# View topic details
docker-compose exec kafka kafka-topics --describe --topic signal-events --bootstrap-server localhost:9092
```

**Database Connection Issues**:
```bash
# Check PostgreSQL status
docker-compose exec postgres psql -U postgres -d eventpipeline -c "\dt"

# View database stats
docker-compose exec postgres psql -U postgres -d eventpipeline -c "SELECT COUNT(*) FROM signals;"
```

**High Memory Usage**:
```bash
# Monitor container resources
docker stats

# Reduce batch sizes in environment variables
# Increase cleanup frequency in anomaly detector
```

**Slow API Responses**:
```bash
# Check database indexes
docker-compose exec postgres psql -U postgres -d eventpipeline -c "\di"

# Monitor slow queries
docker-compose exec postgres psql -U postgres -d eventpipeline -c "SELECT query, mean_time FROM pg_stat_statements ORDER BY mean_time DESC LIMIT 10;"
```

### Scaling Tips

1. **Horizontal Scaling**: Use multiple instances of signal-processor
2. **Database Tuning**: Adjust PostgreSQL settings for high throughput
3. **Kafka Partitioning**: Increase partition count for better parallelism
4. **Memory Management**: Monitor and tune JVM settings for Kafka

## 📄 License

MIT License - see LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## 📞 Support

For questions and support:
- Create an issue in the repository
- Check the troubleshooting section
- Review Grafana dashboards for system insights 