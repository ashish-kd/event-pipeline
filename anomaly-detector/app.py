import asyncio
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any

import asyncpg
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Histogram, start_http_server
import uvicorn
from fastapi import FastAPI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics per specification
anomalies_detected_total = Counter('anomalies_detected_total', 'Total anomalies detected', ['type', 'severity'])
detection_latency_seconds = Histogram('detection_latency_seconds', 'Detection latency in seconds')

class AnomalyDetector:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/eventpipeline')
        self.input_topic = 'signal-events'
        self.output_topic = 'anomaly-events'
        self.group_id = 'anomaly-detector'
        
        # Database connection pool
        self.db_pool = None
        
        # Kafka consumer and producer
        self.consumer = None
        self.producer = None
        
        logger.info("Anomaly detector initialized")
    
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
        """Initialize Kafka consumer and producer."""
        # Consumer
        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=self.kafka_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            max_poll_records=50
        )
        
        # Producer for anomaly events
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=5
        )
        
        logger.info("Kafka consumer and producer initialized")
    
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
                'signal_event_id': signal['event_id']
            })
        
        # Rule 2: Suspicious session pattern (session_id with repeating digits)
        session_id = payload.get('session_id', '')
        if session_id and len(set(session_id.replace('sess_', ''))) <= 2:  # <= 2 unique digits
            anomalies.append({
                'anomaly_type': 'suspicious_session',
                'severity': 1,  # simple: all anomalies same severity
                'signal_event_id': signal['event_id']
            })
        
        # Rule 3: API activity during odd hours (based on current time)
        if signal_type == 'api_call':
            from datetime import datetime
            current_hour = datetime.utcnow().hour
            if current_hour < 6 or current_hour > 22:  # Before 6AM or after 10PM UTC
                anomalies.append({
                    'anomaly_type': 'unusual_time_activity',
                    'severity': 1,  # simple: all anomalies same severity
                    'signal_event_id': signal['event_id']
                })
        
        # Rule 4: Low data value might indicate system issue
        if data_number < 5:
            anomalies.append({
                'anomaly_type': 'low_data_value',
                'severity': 1,  # simple: all anomalies same severity
                'signal_event_id': signal['event_id']
            })
        
        return anomalies
    
    async def process_signal(self, signal: Dict[str, Any]):
        """Process a single signal for anomalies."""
        start_time = time.time()
        
        try:
            anomalies = self.detect_anomalies(signal)
            
            for anomaly in anomalies:
                # Store in database
                await self.store_anomaly(anomaly, signal)
                
                # Publish to Kafka per specification
                await self.publish_anomaly(anomaly)
            
            # Record detection latency
            detection_latency_seconds.observe(time.time() - start_time)
            
        except Exception as e:
            logger.error(f"Error processing signal: {e}")
    
    async def store_anomaly(self, anomaly: Dict[str, Any], original_signal: Dict[str, Any]):
        """Store anomaly in database per schema specification."""
        try:
            insert_query = """
            INSERT INTO anomalies (id, user_id, anomaly_type, severity, detection_ts, signal_event_id, context)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """
            
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    insert_query,
                    str(uuid.uuid4()),  # UUID for anomaly ID
                    original_signal.get('user_id'),
                    anomaly['anomaly_type'],
                    anomaly['severity'],
                    datetime.now(timezone.utc),
                    anomaly['signal_event_id'],
                    json.dumps({'original_signal': original_signal})
                )
            
            logger.info(f"Stored anomaly: {anomaly['anomaly_type']} for signal {anomaly['signal_event_id']}")
            
        except Exception as e:
            logger.error(f"Error storing anomaly: {e}")
    
    async def publish_anomaly(self, anomaly: Dict[str, Any]):
        """Publish anomaly to Kafka topic per specification."""
        try:
            # Send to Kafka with exact format from specification
            self.producer.send(
                self.output_topic,
                value=anomaly,
                key=anomaly['signal_event_id']
            )
            
            # Update metrics with type and severity labels
            severity_str = ['low', 'medium', 'high'][min(anomaly['severity'], 2)]
            anomalies_detected_total.labels(
                type=anomaly['anomaly_type'],
                severity=severity_str
            ).inc()
            
            logger.info(f"Published anomaly: {anomaly['anomaly_type']}")
            
        except Exception as e:
            logger.error(f"Error publishing anomaly: {e}")
    
    async def consume_and_detect(self):
        """Main consumer loop for anomaly detection."""
        logger.info("Starting anomaly detection...")
        
        try:
            while True:
                message_pack = self.consumer.poll(timeout_ms=1000, max_records=50)
                
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        await self.process_signal(message.value)
                
                await asyncio.sleep(0.01)
                
        except Exception as e:
            logger.error(f"Error in anomaly detection loop: {e}")
            raise
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()

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
        "kafka": kafka_status
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
        if detector.producer:
            detector.producer.close()
        if detector.db_pool:
            await detector.db_pool.close()
    logger.info("Anomaly detector shutdown complete")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)