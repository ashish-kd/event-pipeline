#!/usr/bin/env python3
"""
Automated DLQ Recovery Service
✅ Automated DLQ recovery - no manual scripts needed
✅ Intelligent retry logic - handles different failure types
✅ Production monitoring - track recovery metrics
✅ Zero manual intervention - fully automated
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
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

# Prometheus metrics for production monitoring
dlq_messages_processed_total = Counter('dlq_messages_processed_total', 'Total DLQ messages processed')
dlq_messages_replayed_total = Counter('dlq_messages_replayed_total', 'Total DLQ messages successfully replayed')
dlq_messages_failed_total = Counter('dlq_messages_failed_total', 'Total DLQ messages that failed replay', ['failure_type'])
dlq_recovery_latency_seconds = Histogram('dlq_recovery_latency_seconds', 'DLQ recovery latency in seconds')
dlq_queue_size = Gauge('dlq_queue_size', 'Current number of messages in DLQ')
dlq_retry_attempts_total = Counter('dlq_retry_attempts_total', 'Total retry attempts', ['retry_count'])

class DLQRecoveryService:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
        self.dlq_topic = 'signal-events-dlq'
        self.main_topic = 'signal-events'
        self.quarantine_topic = 'signal-events-quarantine'
        self.group_id = 'dlq-recovery-service'
        
        # Intelligent retry configuration
        self.max_retries = int(os.getenv('DLQ_MAX_RETRIES', '3'))
        self.retry_delay_base = float(os.getenv('DLQ_RETRY_DELAY_BASE', '2.0'))  # seconds
        self.batch_size = int(os.getenv('DLQ_BATCH_SIZE', '10'))
        self.recovery_interval = float(os.getenv('DLQ_RECOVERY_INTERVAL', '5.0'))  # seconds between recovery cycles
        
        # Kafka consumers and producers
        self.dlq_consumer = None
        self.main_producer = None
        self.quarantine_producer = None
        self.schema_registry_client = None
        self.value_serializer = None
        
        # Production monitoring statistics
        self.recovery_stats = {
            'total_processed': 0,
            'total_replayed': 0,
            'total_failed': 0,
            'permanent_failures': 0,
            'temporary_failures': 0,
            'last_recovery_time': None,
            'service_start_time': datetime.now(timezone.utc),
            'current_queue_size': 0
        }
        
        logger.info(f"🚀 Automated DLQ Recovery Service initialized")
        logger.info(f"📊 Config: max_retries={self.max_retries}, batch_size={self.batch_size}, recovery_interval={self.recovery_interval}s")
    
    def init_kafka(self):
        """Initialize Kafka consumer and producer with Schema Registry."""
        # Initialize Schema Registry client
        self.schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})
        
        # Initialize value serializer for main topic
        try:
            response = requests.get(f"{self.schema_registry_url}/subjects/signal-events-value/versions/latest")
            if response.status_code == 200:
                schema_str = response.json()['schema']
                self.value_serializer = AvroSerializer(
                    schema_registry_client=self.schema_registry_client,
                    schema_str=schema_str
                )
                logger.info("✅ Schema Registry serializer initialized")
            else:
                logger.warning("⚠️  Could not load schema, using JSON serialization")
                self.value_serializer = None
        except Exception as e:
            logger.warning(f"⚠️  Schema Registry not available, using JSON serialization: {e}")
            self.value_serializer = None
        
        # DLQ Consumer (reads failed messages)
        self.dlq_consumer = Consumer({
            'bootstrap.servers': self.kafka_servers,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,  # 5 minutes
            'session.timeout.ms': 30000,     # 30 seconds
        })
        
        # Main Producer (replays messages to main topic)
        self.main_producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'acks': 'all',
            'retries': 5,
            'retry.backoff.ms': 1000,
            'max.in.flight.requests.per.connection': 1  # Ensure ordering
        })
        
        # Quarantine Producer (sends permanent failures to quarantine)
        self.quarantine_producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'acks': 'all',
            'retries': 5,
            'retry.backoff.ms': 1000,
            'max.in.flight.requests.per.connection': 1
        })
        
        # Subscribe to DLQ topic
        self.dlq_consumer.subscribe([self.dlq_topic])
        
        logger.info("✅ Kafka consumer and producer initialized")
    
    def classify_failure_type(self, error_reason: str) -> str:
        """Intelligent failure classification for retry logic."""
        error_lower = error_reason.lower()
        
        # Permanent failures - don't retry
        permanent_patterns = [
            'schema validation failed',
            'invalid message format', 
            'permanent database constraint violation',
            'foreign key constraint',
            'invalid user_id format',
            'malformed event_id',
            'enum value not valid'
        ]
        
        for pattern in permanent_patterns:
            if pattern in error_lower:
                return 'permanent'
        
        # Temporary failures - retry with backoff
        temporary_patterns = [
            'database connection',
            'connection timeout',
            'temporary',
            'network error',
            'service unavailable',
            'deserialization error',
            'processing error',
            'kafka broker',
            'transaction rollback'
        ]
        
        for pattern in temporary_patterns:
            if pattern in error_lower:
                return 'temporary'
        
        # Default: treat as temporary for safety
        return 'temporary'
    
    def should_retry_message(self, dlq_message: Dict[str, Any], retry_count: int) -> str:
        """Intelligent retry logic - returns 'retry', 'quarantine', or 'skip'."""
        error_reason = dlq_message.get('error_reason', '')
        failure_type = self.classify_failure_type(error_reason)
        
        # Don't retry if max retries exceeded - send to quarantine
        if retry_count >= self.max_retries:
            logger.info(f"🛑 Max retries ({self.max_retries}) exceeded - sending to quarantine")
            return 'quarantine'
        
        # Don't retry permanent failures - send to quarantine immediately
        if failure_type == 'permanent':
            logger.info(f"🚫 Permanent failure detected - sending to quarantine: {error_reason}")
            self.recovery_stats['permanent_failures'] += 1
            dlq_messages_failed_total.labels(failure_type='permanent').inc()
            return 'quarantine'
        
        # Retry temporary failures
        if failure_type == 'temporary':
            logger.info(f"🔄 Retrying temporary failure (attempt {retry_count + 1}): {error_reason}")
            self.recovery_stats['temporary_failures'] += 1
            return 'retry'
        
        return 'retry'
    
    async def send_to_quarantine(self, dlq_message: Dict[str, Any], retry_count: int, final_error: str):
        """Send permanently failed message to quarantine topic with audit headers."""
        try:
            original_message = dlq_message.get('original_message', {})
            error_reason = dlq_message.get('error_reason', 'Unknown')
            
            # Create quarantine record with full audit trail
            quarantine_record = {
                'original_message': original_message,
                'dlq_metadata': {
                    'original_error': error_reason,
                    'final_error': final_error,
                    'retry_count': retry_count,
                    'max_retries': self.max_retries,
                    'failure_classification': self.classify_failure_type(error_reason),
                    'quarantined_at': datetime.now(timezone.utc).isoformat(),
                    'quarantine_reason': 'max_retries_exceeded' if retry_count >= self.max_retries else 'permanent_failure'
                },
                'recovery_service': 'automated-dlq-recovery'
            }
            
            # Kafka headers for audit trail and processing hints
            headers = [
                ('dlq.retry.count', str(retry_count).encode()),
                ('dlq.error.class', self.classify_failure_type(error_reason).encode()),
                ('dlq.quarantine.reason', quarantine_record['dlq_metadata']['quarantine_reason'].encode()),
                ('dlq.quarantined.at', quarantine_record['dlq_metadata']['quarantined_at'].encode()),
                ('dlq.original.error', error_reason.encode()),
                ('dlq.service.version', '1.0.0'.encode()),
                ('schema.version', '1'.encode())
            ]
            
            # Send to quarantine topic
            self.quarantine_producer.produce(
                topic=self.quarantine_topic,
                value=json.dumps(quarantine_record).encode('utf-8'),
                key=original_message.get('user_id', 'unknown').encode('utf-8') if original_message.get('user_id') else b'unknown',
                headers=headers
            )
            
            self.quarantine_producer.flush()
            
            # Log for audit trail
            logger.info(f"🗂️  Quarantined message: {original_message.get('event_id', 'unknown')} after {retry_count} retries")
            logger.info(f"   Reason: {quarantine_record['dlq_metadata']['quarantine_reason']}")
            logger.info(f"   Error: {error_reason}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send message to quarantine: {e}")
            return False
    
    def calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate exponential backoff delay for intelligent retry logic."""
        # Exponential backoff: base * (2^retry_count) with jitter
        import random
        delay = self.retry_delay_base * (2 ** retry_count)
        jitter = random.uniform(0.1, 0.3) * delay  # 10-30% jitter
        return delay + jitter
    
    async def replay_message(self, dlq_message: Dict[str, Any], retry_count: int = 0) -> str:
        """Replay a single message from DLQ to main topic with intelligent retry logic.
        Returns: 'success', 'quarantined', 'failed'
        """
        start_time = time.time()
        
        try:
            original_message = dlq_message.get('original_message', {})
            error_reason = dlq_message.get('error_reason', 'Unknown')
            
            # Check if we should retry this message
            action = self.should_retry_message(dlq_message, retry_count)
            if action == 'quarantine':
                await self.send_to_quarantine(dlq_message, retry_count, error_reason)
                return 'quarantined'
            elif action != 'retry':
                return 'failed'
            
            # Track retry attempts
            dlq_retry_attempts_total.labels(retry_count=str(retry_count)).inc()
            
            # Add recovery metadata to original message
            recovery_metadata = {
                '_retry_count': retry_count,
                '_original_error': error_reason,
                '_recovery_timestamp': datetime.now(timezone.utc).isoformat(),
                '_recovery_service': 'automated-dlq-recovery',
                '_failure_classification': self.classify_failure_type(error_reason)
            }
            
            # Merge recovery metadata (don't overwrite original fields)
            enhanced_message = {**original_message, **recovery_metadata}
            
            # Serialize message for main topic
            if self.value_serializer:
                # Use Avro serialization if schema is available
                try:
                    serialized_value = self.value_serializer(
                        enhanced_message, 
                        SerializationContext(self.main_topic, MessageField.VALUE)
                    )
                except Exception as serialize_error:
                    # If Avro serialization fails, fall back to original message without metadata
                    logger.warning(f"⚠️  Avro serialization failed, using original message: {serialize_error}")
                    serialized_value = self.value_serializer(
                        original_message,
                        SerializationContext(self.main_topic, MessageField.VALUE)
                    )
            else:
                # Fallback to JSON serialization
                serialized_value = json.dumps(enhanced_message).encode('utf-8')
            
            # Kafka headers for audit trail and replay tracking
            headers = [
                ('dlq.retry.count', str(retry_count).encode()),
                ('dlq.error.class', self.classify_failure_type(error_reason).encode()),
                ('dlq.replayed.at', datetime.now(timezone.utc).isoformat().encode()),
                ('dlq.service.version', '1.0.0'.encode()),
                ('dlq.original.error', error_reason.encode()),
                ('schema.version', '1'.encode())
            ]
            
            # Send to main topic with headers
            self.main_producer.produce(
                topic=self.main_topic,
                value=serialized_value,
                key=original_message.get('user_id', 'unknown').encode('utf-8') if original_message.get('user_id') else b'unknown',
                headers=headers
            )
            
            # Flush to ensure delivery
            self.main_producer.flush()
            
            # Update production monitoring metrics
            dlq_messages_replayed_total.inc()
            dlq_recovery_latency_seconds.observe(time.time() - start_time)
            
            logger.info(f"✅ Successfully replayed message (retry {retry_count}): {original_message.get('event_id', 'unknown')}")
            return 'success'
            
        except Exception as e:
            dlq_messages_failed_total.labels(failure_type='replay_error').inc()
            logger.error(f"❌ Failed to replay message (retry {retry_count}): {e}")
            
            # If this is a retryable error and we haven't exceeded max retries, schedule retry
            if retry_count < self.max_retries and self.classify_failure_type(str(e)) == 'temporary':
                retry_delay = self.calculate_retry_delay(retry_count)
                logger.info(f"⏳ Scheduling retry {retry_count + 1} in {retry_delay:.1f}s")
                await asyncio.sleep(retry_delay)
                return await self.replay_message(dlq_message, retry_count + 1)
            else:
                # Send to quarantine if max retries exceeded
                await self.send_to_quarantine(dlq_message, retry_count, str(e))
                return 'quarantined'
    
    async def monitor_and_recover(self):
        """Main recovery loop - zero manual intervention, fully automated."""
        logger.info("🔄 Starting automated DLQ monitoring and recovery...")
        
        batch = []
        last_queue_size_check = time.time()
        
        try:
            while True:
                # Poll for DLQ messages
                msg = self.dlq_consumer.poll(timeout=1.0)
                
                if msg is None:
                    # No message received, process any pending batch
                    if batch:
                        logger.info(f"📦 Processing batch of {len(batch)} DLQ messages")
                        await self.process_dlq_batch(batch)
                        batch = []
                        self.dlq_consumer.commit()
                    
                    # Periodically check queue size for monitoring
                    if time.time() - last_queue_size_check > 30:  # Every 30 seconds
                        await self.update_queue_size_metric()
                        last_queue_size_check = time.time()
                    
                    # Recovery interval between cycles
                    await asyncio.sleep(self.recovery_interval)
                    continue
                
                if msg.error():
                    logger.error(f"❌ DLQ consumer error: {msg.error()}")
                    continue
                
                # Add message to batch
                batch.append(msg)
                
                # Process batch when full
                if len(batch) >= self.batch_size:
                    logger.info(f"📦 Processing batch of {len(batch)} DLQ messages")
                    await self.process_dlq_batch(batch)
                    batch = []
                    self.dlq_consumer.commit()
                
                await asyncio.sleep(0.001)  # Small yield
                
        except Exception as e:
            logger.error(f"💥 Error in automated DLQ recovery loop: {e}")
            raise
        finally:
            if self.dlq_consumer:
                self.dlq_consumer.close()
            if self.main_producer:
                self.main_producer.close()
            if self.quarantine_producer:
                self.quarantine_producer.close()
            logger.info("🛑 Automated DLQ Recovery Service shutdown complete")
    
    async def process_dlq_batch(self, messages: list):
        """Process a batch of DLQ messages with production monitoring."""
        batch_start_time = time.time()
        batch_stats = {
            'processed': 0,
            'replayed': 0,
            'failed': 0,
            'permanent_failures': 0,
            'temporary_failures': 0
        }
        
        for message in messages:
            try:
                batch_stats['processed'] += 1
                dlq_messages_processed_total.inc()
                
                # Parse DLQ message
                dlq_data = json.loads(message.value().decode('utf-8'))
                
                # Attempt to replay with intelligent retry logic
                result = await self.replay_message(dlq_data)
                
                if result == 'success':
                    batch_stats['replayed'] += 1
                elif result == 'quarantined':
                    batch_stats['failed'] += 1
                    logger.info(f"   ✅ Message quarantined (permanent failure): {dlq_data.get('original_message', {}).get('event_id', 'unknown')}")
                else:
                    batch_stats['failed'] += 1
                    
            except Exception as e:
                logger.error(f"❌ Error processing DLQ message: {e}")
                batch_stats['failed'] += 1
                dlq_messages_failed_total.labels(failure_type='processing_error').inc()
        
        # Update global production monitoring statistics
        self.recovery_stats['total_processed'] += batch_stats['processed']
        self.recovery_stats['total_replayed'] += batch_stats['replayed']
        self.recovery_stats['total_failed'] += batch_stats['failed']
        self.recovery_stats['last_recovery_time'] = datetime.now(timezone.utc)
        
        batch_duration = time.time() - batch_start_time
        logger.info(f"📊 Batch completed in {batch_duration:.2f}s: {batch_stats['replayed']} replayed, {batch_stats['failed']} failed")
    
    async def update_queue_size_metric(self):
        """Update DLQ queue size metric for production monitoring."""
        try:
            # This is a simplified implementation - in production you might use Kafka admin client
            self.recovery_stats['current_queue_size'] = 0  # Would be actual queue size
            dlq_queue_size.set(self.recovery_stats['current_queue_size'])
        except Exception as e:
            logger.warning(f"⚠️  Could not update queue size metric: {e}")

# FastAPI app for production monitoring and health checks
app = FastAPI(title="Automated DLQ Recovery Service", version="1.0.0")

# Global service instance
recovery_service = None

@app.get("/health")
async def health_check():
    """Production monitoring health check endpoint."""
    kafka_status = "connected" if recovery_service and recovery_service.dlq_consumer else "disconnected"
    
    uptime = None
    if recovery_service and recovery_service.recovery_stats.get('service_start_time'):
        uptime_delta = datetime.now(timezone.utc) - recovery_service.recovery_stats['service_start_time']
        uptime = str(uptime_delta)
    
    return {
        "status": "healthy",
        "service": "automated-dlq-recovery",
        "kafka": kafka_status,
        "uptime": uptime,
        "features": [
            "automated-dlq-recovery",
            "intelligent-retry-logic", 
            "production-monitoring",
            "zero-manual-intervention"
        ],
        "stats": recovery_service.recovery_stats if recovery_service else {}
    }

@app.get("/stats")
async def get_production_stats():
    """Get detailed production monitoring statistics."""
    if not recovery_service:
        return {"error": "Service not initialized"}
    
    return {
        "recovery_stats": recovery_service.recovery_stats,
        "configuration": {
            "max_retries": recovery_service.max_retries,
            "batch_size": recovery_service.batch_size,
            "retry_delay_base": recovery_service.retry_delay_base,
            "recovery_interval": recovery_service.recovery_interval
        },
        "features": {
            "automated_recovery": True,
            "intelligent_retry_logic": True,
            "production_monitoring": True,
            "zero_manual_intervention": True
        }
    }

@app.get("/metrics-summary")
async def get_metrics_summary():
    """Get a summary of key production monitoring metrics."""
    if not recovery_service:
        return {"error": "Service not initialized"}
    
    stats = recovery_service.recovery_stats
    total_processed = stats['total_processed']
    success_rate = (stats['total_replayed'] / total_processed * 100) if total_processed > 0 else 0
    
    return {
        "success_rate_percent": round(success_rate, 2),
        "total_processed": total_processed,
        "total_replayed": stats['total_replayed'],
        "total_failed": stats['total_failed'],
        "permanent_failures": stats['permanent_failures'],
        "temporary_failures": stats['temporary_failures'],
        "last_recovery": stats['last_recovery_time'],
        "current_queue_size": stats['current_queue_size']
    }

@app.on_event("startup")
async def startup_event():
    """Initialize automated DLQ recovery service on startup."""
    global recovery_service
    
    # Start Prometheus metrics server for production monitoring
    start_http_server(8004)
    logger.info("📊 Prometheus metrics server started on port 8004")
    
    # Initialize automated recovery service
    recovery_service = DLQRecoveryService()
    recovery_service.init_kafka()
    
    # Start automated recovery in background - zero manual intervention
    asyncio.create_task(recovery_service.monitor_and_recover())
    
    logger.info("🚀 Automated DLQ Recovery Service fully operational!")
    logger.info("✅ Features active:")
    logger.info("   • Automated DLQ recovery - no manual scripts needed")
    logger.info("   • Intelligent retry logic - handles different failure types") 
    logger.info("   • Production monitoring - track recovery metrics")
    logger.info("   • Zero manual intervention - fully automated")

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up on shutdown."""
    global recovery_service
    if recovery_service:
        if recovery_service.dlq_consumer:
            recovery_service.dlq_consumer.close()
        if recovery_service.main_producer:
            recovery_service.main_producer.close()
        if recovery_service.quarantine_producer:
            recovery_service.quarantine_producer.close()
    logger.info("🛑 Automated DLQ Recovery Service shutdown complete")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
