#!/bin/bash
# Initialize Kafka topics with proper partitions per specification

echo "Waiting for Kafka to be ready..."
sleep 10

echo "Creating signal-events topic with 6 partitions..."
kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic signal-events \
  --partitions 6 \
  --replication-factor 1

echo "Creating anomaly-events topic with 3 partitions..."
kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic anomaly-events \
  --partitions 3 \
  --replication-factor 1

echo "Creating signal-events-dlq topic for dead letter queue..."
kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic signal-events-dlq \
  --partitions 1 \
  --replication-factor 1

echo "Creating signal-events-quarantine topic for permanent failures..."
kafka-topics --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic signal-events-quarantine \
  --partitions 1 \
  --replication-factor 1

echo "Kafka topics created successfully:"
kafka-topics --bootstrap-server kafka:29092 --list

echo "Topic details:"
kafka-topics --bootstrap-server kafka:29092 --describe --topic signal-events
kafka-topics --bootstrap-server kafka:29092 --describe --topic anomaly-events
kafka-topics --bootstrap-server kafka:29092 --describe --topic signal-events-dlq
kafka-topics --bootstrap-server kafka:29092 --describe --topic signal-events-quarantine

echo ""
echo "🔧 Configuring Production Settings..."

# Function to configure topic with production settings
configure_topic() {
    local topic=$1
    local retention_ms=$2
    local segment_ms=$3
    local cleanup_policy=$4
    
    echo "📝 Configuring topic: $topic"
    
    kafka-configs --bootstrap-server kafka:29092 \
        --entity-type topics \
        --entity-name $topic \
        --alter \
        --add-config "retention.ms=$retention_ms,segment.ms=$segment_ms,cleanup.policy=$cleanup_policy,compression.type=lz4,min.insync.replicas=1"
    
    echo "✅ $topic configured with retention: ${retention_ms}ms, cleanup: $cleanup_policy"
}

# Configure main topics with production retention policies
echo "🗃️  Applying production retention policies..."

# signal-events: 7 days retention (primary events)
configure_topic "signal-events" $((7 * 24 * 60 * 60 * 1000)) $((24 * 60 * 60 * 1000)) "delete"

# anomaly-events: 30 days retention (important events) 
configure_topic "anomaly-events" $((30 * 24 * 60 * 60 * 1000)) $((24 * 60 * 60 * 1000)) "delete"

# signal-events-dlq: 14 days retention (error analysis)
configure_topic "signal-events-dlq" $((14 * 24 * 60 * 60 * 1000)) $((7 * 24 * 60 * 60 * 1000)) "delete"

# signal-events-quarantine: 90 days retention (compliance/audit)
configure_topic "signal-events-quarantine" $((90 * 24 * 60 * 60 * 1000)) $((7 * 24 * 60 * 60 * 1000)) "delete"

# Create and configure Debezium topics with compaction
echo "🔄 Setting up Debezium CDC topics..."

kafka-topics --bootstrap-server kafka:29092 \
    --create --if-not-exists \
    --topic debezium_configs \
    --partitions 1 \
    --replication-factor 1

kafka-topics --bootstrap-server kafka:29092 \
    --create --if-not-exists \
    --topic debezium_offsets \
    --partitions 25 \
    --replication-factor 1

kafka-topics --bootstrap-server kafka:29092 \
    --create --if-not-exists \
    --topic debezium_status \
    --partitions 5 \
    --replication-factor 1

# Configure Debezium topics for compaction (365 days retention)
configure_topic "debezium_configs" $((365 * 24 * 60 * 60 * 1000)) $((7 * 24 * 60 * 60 * 1000)) "compact"
configure_topic "debezium_offsets" $((365 * 24 * 60 * 60 * 1000)) $((24 * 60 * 60 * 1000)) "compact" 
configure_topic "debezium_status" $((30 * 24 * 60 * 60 * 1000)) $((24 * 60 * 60 * 1000)) "compact"

echo ""
echo "🎉 Kafka production configuration completed!"
echo "📋 Retention Summary:"
echo "   • signal-events: 7 days"
echo "   • anomaly-events: 30 days" 
echo "   • DLQ topics: 14 days"
echo "   • Quarantine: 90 days"
echo "   • Debezium: 365 days (compacted)"
echo "   • Compression: LZ4 for all topics"
