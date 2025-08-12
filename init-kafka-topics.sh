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

echo "Kafka topics created successfully:"
kafka-topics --bootstrap-server kafka:29092 --list

echo "Topic details:"
kafka-topics --bootstrap-server kafka:29092 --describe --topic signal-events
kafka-topics --bootstrap-server kafka:29092 --describe --topic anomaly-events
kafka-topics --bootstrap-server kafka:29092 --describe --topic signal-events-dlq
