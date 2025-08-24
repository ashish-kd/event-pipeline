import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any

import asyncpg
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import Counter, Histogram, start_http_server
import uvicorn
from fastapi import FastAPI
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics per specification
anomalies_detected_total = Counter('anomalies_detected_total', 'Total anomalies detected', ['type', 'severity'])
detection_latency_seconds = Histogram('detection_latency_seconds', 'Detection latency in seconds')
outbox_events_total = Counter('outbox_events_total', 'Total events written to outbox')
outbox_write_failures_total = Counter('outbox_write_failures_total', 'Total outbox write failures')

class AnomalyDetector:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
        self.database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/eventpipeline')
        self.input_topic = 'signal-events'
        self.group_id = 'anomaly-detector'
        
        # Database connection pool
        self.db_pool = None
        
        # Kafka consumer only (no producer - using outbox pattern)
        self.consumer = None
        
        logger.info("Anomaly detector initialized with outbox pattern")
    
    async def init_db_pool(self):
        """Initialize database connection pool."""
        try:
            self.db_pool = await asyncpg.create_pool(
                self.database_url,
                min_size=3,
                max_size=10,
                command_timeout=60
            )
            logger.info("Database connection pool initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    def init_kafka(self):
        """Initialize Schema Registry-aware consumer (outbox pattern - no producer needed)."""
        # Initialize Schema Registry client
        self.schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})
        
        # Initialize value deserializer
        self.value_deserializer = AvroDeserializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=self._load_schema_str()
        )
        
        # Regular Kafka Consumer with manual deserialization
        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'max.poll.interval.ms': 300000,  # 5 minutes
        })
        
        # Subscribe to signals topic
        self.consumer.subscribe([self.input_topic])
        
        logger.info("Schema Registry-aware consumer initialized (using outbox pattern for publishing)")
    
    def _load_schema_str(self):
        """Load Avro schema string from Schema Registry."""
        try:
            response = requests.get(f"{self.schema_registry_url}/subjects/signal-events-value/versions/latest")
            if response.status_code == 200:
                schema_data = response.json()
                return schema_data['schema']
            else:
                logger.error(f"Failed to load schema: {response.status_code}")
                raise Exception("Schema not found")
        except Exception as e:
            logger.error(f"Error loading schema: {e}")
            raise
    
    def detect_anomalies(self, signal: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Simple anomaly detection logic based on actual payload structure."""
        anomalies = []
        
        signal_type = signal.get('type')
        payload = signal.get('payload', {})
        user_id = signal.get('user_id', '')
        
        # Extract number from data field (e.g., "sample_data_42" -> 42)
        data_str = payload.get('data', '')
        try:
            data_number = int(data_str.split('_')[-1]) if '_' in data_str else 0
        except (ValueError, IndexError):
            data_number = 0
        
        # Rule 1: High data value anomaly (>96 as suggested)
        if data_number > 96:
            anomalies.append({
                'anomaly_type': 'high_data_value',
                'severity': 1,  # simple: all anomalies same severity
                'signal_event_id': signal['event_id'],
                'user_id': user_id
            })
        
        # Rule 2: Suspicious session pattern (session_id with repeating digits)
        session_id = payload.get('session_id', '')
        if session_id and len(set(session_id.replace('sess_', ''))) <= 2:  # <= 2 unique digits
            anomalies.append({
                'anomaly_type': 'suspicious_session',
                'severity': 1,  # simple: all anomalies same severity
                'signal_event_id': signal['event_id'],
                'user_id': user_id
            })
        
        # Rule 3: API activity during odd hours (based on current time)
        if signal_type == 'api_call':
            from datetime import datetime
            current_hour = datetime.utcnow().hour
            if current_hour < 6 or current_hour > 22:  # Before 6AM or after 10PM UTC
                anomalies.append({
                    'anomaly_type': 'unusual_time_activity',
                    'severity': 1,  # simple: all anomalies same severity
                    'signal_event_id': signal['event_id'],
                    'user_id': user_id
                })
        
        # Rule 4: Low data value might indicate system issue
        if data_number < 5:
            anomalies.append({
                'anomaly_type': 'low_data_value',
                'severity': 1,  # simple: all anomalies same severity
                'signal_event_id': signal['event_id'],
                'user_id': user_id
            })
        
        return anomalies
    
    async def process_signal(self, signal: Dict[str, Any]):
        """Process a single signal for anomalies using atomic outbox pattern."""
        start_time = time.time()
        
        try:
            anomalies = self.detect_anomalies(signal)
            
            if anomalies:
                # Atomic transaction: store anomaly + outbox event
                await self.store_anomaly_atomic(anomalies, signal)
            
            # Record detection latency
            detection_latency_seconds.observe(time.time() - start_time)
            
        except Exception as e:
            logger.error(f"Error processing signal: {e}")
            outbox_write_failures_total.inc()
    
    async def store_anomaly_atomic(self, anomalies: List[Dict[str, Any]], original_signal: Dict[str, Any]):
        """Store signal first, then anomalies and outbox events atomically in a single transaction."""
        try:
            # Define SQL queries
            signal_upsert_query = """
            INSERT INTO signals (id, user_id, source, type, event_ts, ingest_ts, payload)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING
            """
            
            anomaly_insert_query = """
            INSERT INTO anomalies (id, user_id, anomaly_type, severity, detection_ts, signal_event_id, context)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """
            
            outbox_insert_query = """
            INSERT INTO outbox_events (event_type, aggregate_id, aggregate_type, payload, created_at)
            VALUES ($1, $2, $3, $4, $5)
            """
            
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():  # Atomic transaction
                    # 1. FIRST: Ensure signal exists in signals table (handles race condition)
                    event_ts = original_signal['event_ts']
                    ingest_ts = original_signal['ingest_ts']
                    
                    # Ensure timestamps have timezone info
                    if event_ts.tzinfo is None:
                        event_ts = event_ts.replace(tzinfo=timezone.utc)
                    if ingest_ts.tzinfo is None:
                        ingest_ts = ingest_ts.replace(tzinfo=timezone.utc)
                    
                    await conn.execute(
                        signal_upsert_query,
                        original_signal['event_id'],
                        original_signal['user_id'],
                        original_signal['source'],
                        original_signal['type'],
                        event_ts,
                        ingest_ts,
                        json.dumps(original_signal['payload'])
                    )
                    
                    # 2. THEN: Store anomalies (FK constraint will now pass)
                    for anomaly in anomalies:
                        anomaly_id = str(uuid.uuid4())
                        detection_ts = datetime.now(timezone.utc)
                        
                        # Simplified context for anomaly
                        simple_context = {
                            'signal_id': str(original_signal.get('event_id', '')),
                            'signal_type': str(original_signal.get('type', '')),
                            'user_id': str(original_signal.get('user_id', ''))
                        }
                        
                        await conn.execute(
                            anomaly_insert_query,
                            anomaly_id,
                            original_signal.get('user_id'),
                            anomaly['anomaly_type'],
                            anomaly['severity'],
                            detection_ts,
                            anomaly['signal_event_id'],
                            json.dumps(simple_context)
                        )
                        
                        # 3. Store event in outbox (same transaction)
                        outbox_payload = {
                            'anomaly_type': str(anomaly['anomaly_type']),
                            'severity': int(anomaly['severity']),
                            'signal_event_id': str(anomaly['signal_event_id']),
                            'detection_ts': int(detection_ts.timestamp() * 1000),  # milliseconds
                            'user_id': str(anomaly.get('user_id', '')),
                            'anomaly_id': str(anomaly_id)
                        }
                        
                        await conn.execute(
                            outbox_insert_query,
                            'anomaly_detected',  # event_type
                            anomaly_id,          # aggregate_id
                            'anomaly',           # aggregate_type
                            json.dumps(outbox_payload),
                            detection_ts
                        )
                        
                        # Update metrics
                        severity_str = ['low', 'medium', 'high'][min(anomaly['severity'], 2)]
                        anomalies_detected_total.labels(
                            type=anomaly['anomaly_type'],
                            severity=severity_str
                        ).inc()
                        
                        outbox_events_total.inc()
                        
                        logger.info(f"Stored anomaly: {anomaly['anomaly_type']} for signal {anomaly['signal_event_id']}")
            
        except Exception as e:
            logger.error(f"Error storing anomaly: {e}")
            outbox_write_failures_total.inc()
            raise
    

    
    async def consume_and_detect(self):
        """Main consumer loop for anomaly detection with Schema Registry-aware consumer."""
        logger.info("Starting anomaly detection...")
        
        try:
            while True:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                try:
                    # Manually deserialize the message using Schema Registry
                    deserialized_value = self.value_deserializer(
                        msg.value(), 
                        SerializationContext(msg.topic(), MessageField.VALUE)
                    )
                    await self.process_signal(deserialized_value)
                    
                except Exception as e:
                    logger.error(f"Error deserializing message: {e}")
                    # Continue processing other messages
                
                await asyncio.sleep(0.001)  # Small yield
                
        except Exception as e:
            logger.error(f"Error in anomaly detection loop: {e}")
            raise
        finally:
            if self.consumer:
                self.consumer.close()

# FastAPI app for health checks only
app = FastAPI(title="Anomaly Detector Service", version="1.0.0")

# Global detector instance
detector = None

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    db_status = "connected" if detector and detector.db_pool else "disconnected"
    kafka_status = "connected" if detector and detector.consumer else "disconnected"
    
    return {
        "status": "healthy",
        "service": "anomaly-detector",
        "database": db_status,
        "kafka": kafka_status,
        "pattern": "outbox"
    }

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    global detector
    
    # Start Prometheus metrics server
    start_http_server(8001)
    logger.info("Prometheus metrics server started on port 8001")
    
    # Initialize detector
    detector = AnomalyDetector()
    await detector.init_db_pool()
    detector.init_kafka()
    
    # Start detection in background
    asyncio.create_task(detector.consume_and_detect())

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown."""
    global detector
    if detector:
        if detector.consumer:
            detector.consumer.close()
        if detector.db_pool:
            await detector.db_pool.close()
    logger.info("Anomaly detector shutdown complete")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)