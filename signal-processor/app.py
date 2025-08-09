import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import List, Dict, Any

import asyncpg
from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import uvicorn
from fastapi import FastAPI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
events_processed = Counter('events_processed_total', 'Total events processed', ['status'])
processing_latency = Histogram('processing_latency_seconds', 'Event processing latency')
batch_size_gauge = Gauge('batch_size_current', 'Current batch size being processed')
db_connection_pool_size = Gauge('db_connection_pool_size', 'Database connection pool size')

class SignalProcessor:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/eventpipeline')
        self.batch_size = int(os.getenv('BATCH_SIZE', '100'))
        self.topic = 'signal-events'
        self.group_id = 'signal-processor'
        
        # Database connection pool
        self.db_pool = None
        
        # Consumer
        self.consumer = None
        
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
            db_connection_pool_size.set(20)  # max_size
            
            # Ensure tables exist
            await self.create_tables()
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    async def create_tables(self):
        """Create database tables if they don't exist."""
        create_signals_table = """
        CREATE TABLE IF NOT EXISTS signals (
            id BIGSERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            type VARCHAR(50) NOT NULL,
            timestamp BIGINT NOT NULL,
            payload JSONB NOT NULL,
            processed_at TIMESTAMP DEFAULT NOW(),
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_signals_user_id ON signals(user_id);
        CREATE INDEX IF NOT EXISTS idx_signals_type ON signals(type);
        CREATE INDEX IF NOT EXISTS idx_signals_timestamp ON signals(timestamp);
        CREATE INDEX IF NOT EXISTS idx_signals_processed_at ON signals(processed_at);
        CREATE INDEX IF NOT EXISTS idx_signals_payload_gin ON signals USING GIN(payload);
        """
        
        create_processing_stats = """
        CREATE TABLE IF NOT EXISTS processing_stats (
            id BIGSERIAL PRIMARY KEY,
            batch_size INTEGER NOT NULL,
            processing_time_ms INTEGER NOT NULL,
            events_count INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        
        async with self.db_pool.acquire() as conn:
            await conn.execute(create_signals_table)
            await conn.execute(create_processing_stats)
            logger.info("Database tables created/verified")
    
    def init_kafka_consumer(self):
        """Initialize Kafka consumer."""
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_servers,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=False,
            max_poll_records=self.batch_size,
            fetch_min_bytes=1024,
            fetch_max_wait_ms=500,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        logger.info(f"Kafka consumer initialized for topic: {self.topic}")
    
    async def process_batch(self, messages: List[Dict[str, Any]]) -> bool:
        """Process a batch of messages and store in database."""
        if not messages:
            return True
        
        start_time = time.time()
        batch_size_gauge.set(len(messages))
        
        try:
            # Prepare data for batch insert
            records = []
            for msg in messages:
                records.append((
                    msg['user_id'],
                    msg['type'],
                    msg['timestamp'],
                    json.dumps(msg['payload'])
                ))
            
            # Batch insert to database
            insert_query = """
            INSERT INTO signals (user_id, type, timestamp, payload)
            VALUES ($1, $2, $3, $4::jsonb)
            """

            # Chunk and insert concurrently using the pool
            chunk_size = 500
            chunks = [records[i:i+chunk_size] for i in range(0, len(records), chunk_size)]
            async def insert_chunk(chunk):
                async with self.db_pool.acquire() as conn:
                    await conn.executemany(insert_query, chunk)
            await asyncio.gather(*(insert_chunk(c) for c in chunks))
            
            # Record processing stats
            processing_time_ms = int((time.time() - start_time) * 1000)
            stats_query = """
            INSERT INTO processing_stats (batch_size, processing_time_ms, events_count)
            VALUES ($1, $2, $3)
            """
            
            async with self.db_pool.acquire() as conn:
                await conn.execute(stats_query, len(messages), processing_time_ms, len(messages))
            
            # Update metrics
            events_processed.labels(status='success').inc(len(messages))
            processing_latency.observe(time.time() - start_time)
            
            logger.info(f"Processed batch of {len(messages)} events in {processing_time_ms}ms")
            return True
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            events_processed.labels(status='error').inc(len(messages))
            return False
    
    async def handle_failed_messages(self, messages: List[Dict[str, Any]]):
        """Handle messages that failed to process (Dead Letter Queue logic)."""
        logger.warning(f"Handling {len(messages)} failed messages")
        
        # In a production system, you might:
        # 1. Send to a dead letter topic
        # 2. Store in a failed_events table
        # 3. Send alerts
        
        try:
            failed_events_query = """
            CREATE TABLE IF NOT EXISTS failed_events (
                id BIGSERIAL PRIMARY KEY,
                original_message JSONB NOT NULL,
                error_reason TEXT,
                failed_at TIMESTAMP DEFAULT NOW(),
                retry_count INTEGER DEFAULT 0
            );
            """
            
            async with self.db_pool.acquire() as conn:
                await conn.execute(failed_events_query)
                
                for msg in messages:
                    await conn.execute(
                        "INSERT INTO failed_events (original_message, error_reason) VALUES ($1, $2)",
                        json.dumps(msg), "Processing failed"
                    )
            
            logger.info(f"Stored {len(messages)} failed messages in failed_events table")
            
        except Exception as e:
            logger.error(f"Error handling failed messages: {e}")
    
    async def consume_and_process(self):
        """Main consumer loop with batch processing."""
        logger.info("Starting message consumption...")
        
        batch = []
        last_commit = time.time()
        commit_interval = 5.0  # Commit every 5 seconds
        
        try:
            while True:
                # Poll for messages
                message_pack = self.consumer.poll(timeout_ms=1000, max_records=self.batch_size)
                
                for topic_partition, messages in message_pack.items():
                    for message in messages:
                        try:
                            # Validate message structure
                            required_fields = ['user_id', 'type', 'timestamp', 'payload']
                            if not all(field in message.value for field in required_fields):
                                logger.warning(f"Invalid message structure: {message.value}")
                                continue
                            
                            batch.append(message.value)
                            
                            # Process batch when full
                            if len(batch) >= self.batch_size:
                                success = await self.process_batch(batch)
                                if not success:
                                    await self.handle_failed_messages(batch)
                                batch = []
                                
                                # Commit offsets
                                self.consumer.commit()
                                last_commit = time.time()
                        
                        except Exception as e:
                            logger.error(f"Error processing individual message: {e}")
                            events_processed.labels(status='error').inc()
                
                # Process remaining batch if timeout reached
                current_time = time.time()
                if batch and (current_time - last_commit) > commit_interval:
                    success = await self.process_batch(batch)
                    if not success:
                        await self.handle_failed_messages(batch)
                    batch = []
                    
                    # Commit offsets
                    self.consumer.commit()
                    last_commit = current_time
                
                # Small sleep to prevent busy waiting
                await asyncio.sleep(0.01)
                
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}")
            raise
        finally:
            # Process any remaining messages
            if batch:
                await self.process_batch(batch)
                self.consumer.commit()
            
            if self.consumer:
                self.consumer.close()
                logger.info("Kafka consumer closed")

# FastAPI app for health checks and metrics
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
        "kafka": kafka_status,
        "batch_size": int(os.getenv('BATCH_SIZE', '100'))
    }

@app.get("/metrics/custom")
async def custom_metrics():
    """Custom application metrics."""
    if not processor or not processor.db_pool:
        return {"error": "Database not initialized"}
    
    try:
        async with processor.db_pool.acquire() as conn:
            # Get processing statistics
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_events,
                    AVG(processing_time_ms) as avg_processing_time_ms,
                    MAX(processing_time_ms) as max_processing_time_ms
                FROM processing_stats 
                WHERE created_at > NOW() - INTERVAL '1 hour'
            """)
            
            # Get recent signal counts by type
            signal_counts = await conn.fetch("""
                SELECT type, COUNT(*) as count
                FROM signals 
                WHERE created_at > NOW() - INTERVAL '1 hour'
                GROUP BY type
                ORDER BY count DESC
            """)
            
            return {
                "total_events_last_hour": stats['total_events'] or 0,
                "avg_processing_time_ms": float(stats['avg_processing_time_ms'] or 0),
                "max_processing_time_ms": stats['max_processing_time_ms'] or 0,
                "signal_counts_by_type": [dict(row) for row in signal_counts]
            }
    except Exception as e:
        logger.error(f"Error getting custom metrics: {e}")
        return {"error": str(e)}

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
    processor.init_kafka_consumer()
    
    # Start consumption in background
    asyncio.create_task(processor.consume_and_process())

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown."""
    global processor
    if processor:
        if processor.consumer:
            processor.consumer.close()
        if processor.db_pool:
            await processor.db_pool.close()
    logger.info("Signal processor shutdown complete")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 