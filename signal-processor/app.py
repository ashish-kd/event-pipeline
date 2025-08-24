import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Dict, Any

import asyncpg
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import Counter, Histogram, Gauge, start_http_server
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
signals_consumed_total = Counter('signals_consumed_total', 'Total signals consumed')
db_write_failures_total = Counter('db_write_failures_total', 'Total database write failures')
processing_latency_seconds = Histogram('processing_latency_seconds', 'Processing latency in seconds')
consumer_lag = Gauge('consumer_lag', 'Consumer lag')
signals_dlq_total = Counter('signals_dlq_total', 'Total signals sent to DLQ')

class SignalProcessor:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
        self.database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/eventpipeline')
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.topic = 'signal-events'
        self.dlq_topic = 'signal-events-dlq'
        self.group_id = 'signal-processor'
        
        # Database connection pool
        self.db_pool = None
        
        # Kafka consumer and DLQ producer
        self.consumer = None
        self.dlq_producer = None
        
        logger.info(f"Signal processor initialized - batch size: {self.batch_size}")
    
    async def init_db_pool(self):
        """Initialize database connection pool."""
        try:
            self.db_pool = await asyncpg.create_pool(
                self.database_url,
                min_size=5,
                max_size=20,
                command_timeout=60
            )
            logger.info("Database connection pool initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    def init_kafka(self):
        """Initialize Schema Registry-aware consumer and DLQ producer."""
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
            'enable.auto.commit': False,  # Per specification
            'max.poll.interval.ms': 300000,  # 5 minutes
        })
        
        # Subscribe to topic
        self.consumer.subscribe([self.topic])
        
        # DLQ Producer (regular producer for error messages)
        self.dlq_producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'acks': 'all',
            'retries': 5
        })
        
        logger.info(f"Schema Registry-aware consumer and DLQ producer initialized")
    
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
    
    async def process_batch(self, messages: List[Dict[str, Any]]) -> bool:
        """Process batch with idempotent upserts per specification."""
        if not messages:
            return True
        
        start_time = time.time()
        
        try:
            # Avro consumer already validated schema, so all messages are valid
            # Just process them directly
            
            # Idempotent upsert with enrichment support
            upsert_query = """
            INSERT INTO signals (id, user_id, source, type, event_ts, ingest_ts, payload)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE SET
                payload = EXCLUDED.payload,
                ingest_ts = CASE 
                    WHEN EXCLUDED.ingest_ts > signals.ingest_ts 
                    THEN EXCLUDED.ingest_ts 
                    ELSE signals.ingest_ts 
                END
            """
            
            async with self.db_pool.acquire() as conn:
                for msg in messages:
                    try:
                        # Timestamps are already datetime objects from Avro deserialization
                        event_ts = msg['event_ts']
                        ingest_ts = msg['ingest_ts']
                        
                        # Ensure they have timezone info
                        if event_ts.tzinfo is None:
                            event_ts = event_ts.replace(tzinfo=timezone.utc)
                        if ingest_ts.tzinfo is None:
                            ingest_ts = ingest_ts.replace(tzinfo=timezone.utc)
                        
                        await conn.execute(
                            upsert_query,
                            msg['event_id'],
                            msg['user_id'],
                            msg['source'],
                            msg['type'],
                            event_ts,
                            ingest_ts,
                            json.dumps(msg['payload'])
                        )
                    except Exception as e:
                        logger.error(f"Database write error for event {msg['event_id']}: {e}")
                        db_write_failures_total.inc()
                        await self.send_to_dlq(msg, f"Database write error: {e}")
            
            # Update metrics
            signals_consumed_total.inc(len(messages))
            processing_latency_seconds.observe(time.time() - start_time)
            
            logger.info(f"Processed batch of {len(messages)} events")
            return True
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            db_write_failures_total.inc(len(messages))
            
            # Send entire batch to DLQ on major failure
            for msg in messages:
                await self.send_to_dlq(msg, f"Batch processing error: {e}")
            
            return False
    
    async def send_to_dlq(self, message: Dict[str, Any], error_reason: str):
        """Send message to DLQ topic (as JSON since it's error data)."""
        try:
            dlq_message = {
                'original_message': message,
                'error_reason': error_reason,
                'failed_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Use produce with JSON serialization for DLQ
            self.dlq_producer.produce(
                topic=self.dlq_topic,
                value=json.dumps(dlq_message).encode('utf-8'),
                key=message.get('user_id', 'unknown').encode('utf-8') if message.get('user_id') else b'unknown'
            )
            
            signals_dlq_total.inc()
            logger.info(f"Sent message to DLQ: {error_reason}")
            
        except Exception as e:
            logger.error(f"Error sending to DLQ: {e}")
    
    async def consume_and_process(self):
        """Main consumer loop with Schema Registry-aware consumer."""
        logger.info("Starting message consumption...")
        
        batch = []
        
        try:
            while True:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No message received, process any pending batch
                    if batch:
                        await self.process_batch(batch)
                        batch = []
                        self.consumer.commit()
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
                    batch.append(deserialized_value)
                    
                    # Update consumer lag metric
                    consumer_lag.set(0)  # Simplified for now
                    
                    # Process batch when full
                    if len(batch) >= self.batch_size:
                        await self.process_batch(batch)
                        batch = []
                        
                        # Commit after successful DB write per specification
                        self.consumer.commit()
                        
                except Exception as e:
                    logger.error(f"Error deserializing message: {e}")
                    # Send to DLQ if deserialization fails
                    await self.send_to_dlq({"raw_message": "deserialization_failed"}, f"Deserialization error: {e}")
                
                await asyncio.sleep(0.001)  # Small yield
                
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            raise
        finally:
            if self.consumer:
                self.consumer.close()
            logger.info("Kafka connections closed")

# FastAPI app for health checks only
app = FastAPI(title="Signal Processor Service", version="1.0.0")

# Global processor instance
processor = None

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    db_status = "connected" if processor and processor.db_pool else "disconnected"
    kafka_status = "connected" if processor and processor.consumer else "disconnected"
    
    return {
        "status": "healthy",
        "service": "signal-processor",
        "database": db_status,
        "kafka": kafka_status
    }

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    global processor
    
    # Start Prometheus metrics server
    start_http_server(8001)
    logger.info("Prometheus metrics server started on port 8001")
    
    # Initialize processor
    processor = SignalProcessor()
    await processor.init_db_pool()
    processor.init_kafka()
    
    # Start consumption in background
    asyncio.create_task(processor.consume_and_process())

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown."""
    global processor
    if processor:
        if processor.consumer:
            processor.consumer.close()
        if processor.dlq_producer:
            processor.dlq_producer.close()
        if processor.db_pool:
            await processor.db_pool.close()
    logger.info("Signal processor shutdown complete")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)