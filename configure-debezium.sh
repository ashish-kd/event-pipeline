#!/bin/bash
# Configure Debezium Outbox Connector for Event Pipeline

set -e

DEBEZIUM_URL="http://debezium-connect:8083"
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "Configuring Debezium Outbox Connector..."

# Wait for Debezium Connect to be ready
wait_for_debezium() {
    echo "Waiting for Debezium Connect to be ready..."
    for i in $(seq 1 $MAX_RETRIES); do
        if curl -f -s "$DEBEZIUM_URL/connectors" > /dev/null 2>&1; then
            echo "Debezium Connect is ready!"
            return 0
        fi
        echo "Attempt $i/$MAX_RETRIES: Debezium not ready yet, waiting ${RETRY_INTERVAL}s..."
        sleep $RETRY_INTERVAL
    done
    echo "ERROR: Debezium Connect failed to become ready after $MAX_RETRIES attempts"
    exit 1
}

# Configure outbox connector
configure_outbox_connector() {
    echo "Configuring outbox connector..."
    
    cat > /tmp/outbox-connector.json << EOF
{
    "name": "outbox-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "postgres",
        "database.dbname": "eventpipeline",
        "database.server.name": "eventpipeline-outbox",
        "topic.prefix": "eventpipeline-outbox",
        "table.include.list": "public.outbox_events",
        "transforms": "outbox",
        "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
        "transforms.outbox.route.topic.replacement": "anomaly-events",
        "transforms.outbox.table.field.event.id": "id",
        "transforms.outbox.table.field.event.key": "aggregateid",
        "transforms.outbox.table.field.event.payload": "payload",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_outbox_slot",
        "publication.name": "debezium_outbox_publication",
        "tombstones.on.delete": false,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": false
    }
}
EOF

    # Create the connector
    curl -X POST \
        -H "Content-Type: application/json" \
        -d @/tmp/outbox-connector.json \
        "$DEBEZIUM_URL/connectors"
    
    if [ $? -eq 0 ]; then
        echo "✅ Outbox connector configured successfully!"
    else
        echo "❌ Failed to configure outbox connector"
        exit 1
    fi
}

# Check connector status
check_connector_status() {
    echo "Checking connector status..."
    curl -s "$DEBEZIUM_URL/connectors/outbox-connector/status"
    echo ""  # Add newline for readability
}

# Main execution
main() {
    wait_for_debezium
    sleep 10  # Additional wait for database to be fully ready
    configure_outbox_connector
    sleep 5   # Wait for connector to initialize
    check_connector_status
    
    echo ""
    echo "🎉 Debezium Outbox Connector configured successfully!"
    echo "📊 Connector will now publish outbox events to 'anomaly-events' topic"
    echo "🔍 Monitor status: curl http://localhost:8083/connectors/outbox-connector/status"
}

main "$@"
