import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import List, Dict, Any
from dateutil import parser as date_parser

import asyncpg
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import uvicorn
from fastapi import FastAPI

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
        """Initialize Kafka consumer and DLQ producer."""
        # Consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=False,  # Per specification
            max_poll_records=self.batch_size
        )
        
        # DLQ Producer
        self.dlq_producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=5
        )
        
        logger.info(f"Kafka consumer and DLQ producer initialized")
    
    async def process_batch(self, messages: List[Dict[str, Any]]) -> bool:
        """Process batch with idempotent upserts per specification."""
        if not messages:
            return True
        
        start_time = time.time()
        
        try:
            valid_messages = []
            invalid_messages = []
            
            # Validate messages
            for msg in messages:
                required_fields = ['event_id', 'user_id', 'source', 'type', 'event_ts', 'ingest_ts', 'payload']
                if all(field in msg for field in required_fields):
                    valid_messages.append(msg)
                else:
                    invalid_messages.append(msg)
                    logger.warning(f"Invalid message structure: {msg}")
            
            # Send invalid messages to DLQ
            for invalid_msg in invalid_messages:
                await self.send_to_dlq(invalid_msg, "Invalid message structure")
            
            if not valid_messages:
                return True
            
            # Idempotent upsert per specification
            upsert_query = """
            INSERT INTO signals (id, user_id, source, type, event_ts, ingest_ts, payload)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING
            """
            
            async with self.db_pool.acquire() as conn:
                for msg in valid_messages:
                    try:
                        # Parse ISO timestamp strings to datetime objects
                        event_ts = date_parser.parse(msg['event_ts'])
                        ingest_ts = date_parser.parse(msg['ingest_ts'])
                        
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
            
            logger.info(f"Processed batch of {len(valid_messages)} valid events")
            return True
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            db_write_failures_total.inc(len(messages))
            
            # Send entire batch to DLQ on major failure
            for msg in messages:
                await self.send_to_dlq(msg, f"Batch processing error: {e}")
            
            return False
    
    async def send_to_dlq(self, message: Dict[str, Any], error_reason: str):
        """Send message to DLQ topic."""
        try:
            dlq_message = {
                'original_message': message,
                'error_reason': error_reason,
                'failed_at': datetime.utcnow().isoformat()
            }
            
            self.dlq_producer.send(
                self.dlq_topic,
                value=dlq_message,
                key=message.get('user_id', 'unknown')
            )
            
            signals_dlq_total.inc()
            logger.info(f"Sent message to DLQ: {error_reason}")
            
        except Exception as e:
            logger.error(f"Error sending to DLQ: {e}")
    
    async def consume_and_process(self):
        """Main consumer loop."""
        logger.info("Starting message consumption...")
        
        batch = []
        
        try:
            while True:
                # Poll for messages
                message_pack = self.consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        batch.append(message.value)
                        
                        # Process batch when full
                        if len(batch) >= self.batch_size:
                            await self.process_batch(batch)
                            batch = []
                            
                            # Commit after successful DB write per specification
                            self.consumer.commit()
                
                # Process remaining batch if any
                if batch:
                    await self.process_batch(batch)
                    batch = []
                    self.consumer.commit()
                
                await asyncio.sleep(0.01)
                
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            raise
        finally:
            if self.consumer:
                self.consumer.close()
            if self.dlq_producer:
                self.dlq_producer.close()
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