# Event Pipeline Makefile
# Developer experience targets per specification

.PHONY: up down load logs clean help

# Start the entire stack
up:
	@echo "Starting event pipeline stack..."
	docker compose up -d --build

# Stop the stack
down:
	@echo "Stopping event pipeline stack..."
	docker compose down

# Stop and clean volumes
clean:
	@echo "Stopping and cleaning volumes..."
	docker compose down -v

# Load test - send 1k events
load:
	@echo "Loading 1k test events..."
	@for i in $$(seq 1 1000); do \
		curl -s -X POST http://localhost:8001/health > /dev/null || true; \
	done
	@echo "Load test completed"

# View logs for all services
logs:
	@echo "Following logs for all services..."
	docker compose logs -f

# View logs for specific service
logs-emitter:
	docker compose logs -f signal-emitter

logs-processor:
	docker compose logs -f signal-processor

logs-anomaly:
	docker compose logs -f anomaly-detector

logs-kafka:
	docker compose logs -f kafka

logs-postgres:
	docker compose logs -f postgres

# Check service status
status:
	@echo "Service status:"
	docker compose ps

# Health checks
health:
	@echo "Checking service health..."
	@curl -s http://localhost:8001/health | jq . || echo "Signal Emitter: DOWN"
	@curl -s http://localhost:8002/health | jq . || echo "Signal Processor: DOWN"
	@curl -s http://localhost:8003/health | jq . || echo "Anomaly Detector: DOWN"

# View database stats
db-stats:
	@echo "Database statistics:"
	docker compose exec postgres psql -U postgres -d eventpipeline -c "SELECT COUNT(*) as signal_count FROM signals;"
	docker compose exec postgres psql -U postgres -d eventpipeline -c "SELECT COUNT(*) as anomaly_count FROM anomalies;"

# Kafka topic info
kafka-topics:
	@echo "Kafka topics:"
	docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# DLQ Recovery Service commands
dlq-status:
	@echo "Checking automated DLQ recovery status..."
	@curl -s http://localhost:8005/health | jq . || echo "DLQ Recovery Service: DOWN"

dlq-stats:
	@echo "DLQ Recovery Statistics:"
	@curl -s http://localhost:8005/stats | jq . || echo "Stats not available"

dlq-metrics:
	@echo "DLQ Recovery Metrics Summary:"
	@curl -s http://localhost:8005/metrics-summary | jq . || echo "Metrics not available"

logs-dlq:
	docker compose logs -f dlq-recovery

# Outbox pattern monitoring
outbox-status:
	@echo "Checking outbox pattern status..."
	@echo "=== Debezium Connector Status ==="
	@curl -s http://localhost:8083/connectors/outbox-connector/status | grep -E '"state"|"type"|"errors"' || echo "Connector not available"
	@echo ""
	@echo "=== Outbox Events in Database ==="
	docker compose exec postgres psql -U postgres -d eventpipeline -c "SELECT COUNT(*) as pending_events FROM outbox_events WHERE processed_at IS NULL;"
	@echo "=== Recent Outbox Events ==="
	docker compose exec postgres psql -U postgres -d eventpipeline -c "SELECT eventtype, aggregateid, created_at FROM outbox_events ORDER BY created_at DESC LIMIT 5;"

debezium-config:
	@echo "Reconfiguring Debezium connector..."
	@./configure-debezium.sh

# Help
help:
	@echo "Event Pipeline Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  up            Start the entire stack"
	@echo "  down          Stop the stack"
	@echo "  clean         Stop and clean volumes"
	@echo "  load          Run load test (1k events)"
	@echo "  logs          View logs for all services"
	@echo "  logs-<service> View logs for specific service"
	@echo "  status        Check service status"
	@echo "  health        Check service health endpoints"
	@echo "  db-stats      View database statistics"
	@echo "  kafka-topics  List Kafka topics"
	@echo "  dlq-status    Check automated DLQ recovery status"
	@echo "  dlq-stats     View DLQ recovery statistics"
	@echo "  dlq-metrics   View DLQ recovery metrics summary"
	@echo "  logs-dlq      View DLQ recovery service logs"
	@echo "  outbox-status Check outbox pattern and Debezium status"
	@echo "  debezium-config Reconfigure Debezium connector"
	@echo "  help          Show this help"
