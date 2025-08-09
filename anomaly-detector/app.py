import asyncio
import json
import logging
import os
import time
from asyncio import Queue
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

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

# Prometheus metrics
anomalies_detected = Counter('anomalies_detected_total', 'Total anomalies detected', ['anomaly_type'])
detection_latency = Histogram('anomaly_detection_latency_seconds', 'Anomaly detection latency')
active_user_sessions = Gauge('active_user_sessions', 'Number of active user sessions being monitored')

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
        
        # In-memory tracking for real-time detection
        self.user_sessions = defaultdict(lambda: {
            'login_attempts': deque(),
            'api_failures': deque(),
            'recent_purchases': deque(),
            'last_activity': None
        })
        
        # Cleanup interval for old sessions
        self.last_cleanup = time.time()
        self.cleanup_interval = 300  # 5 minutes
        
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
            
            # Ensure tables exist
            await self.create_tables()
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            raise
    
    async def create_tables(self):
        """Create database tables if they don't exist."""
        create_anomalies_table = """
        CREATE TABLE IF NOT EXISTS anomalies (
            id BIGSERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL,
            anomaly_type VARCHAR(100) NOT NULL,
            severity VARCHAR(20) NOT NULL,
            description TEXT NOT NULL,
            original_signal_data JSONB,
            detection_timestamp BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_anomalies_user_id ON anomalies(user_id);
        CREATE INDEX IF NOT EXISTS idx_anomalies_type ON anomalies(anomaly_type);
        CREATE INDEX IF NOT EXISTS idx_anomalies_severity ON anomalies(severity);
        CREATE INDEX IF NOT EXISTS idx_anomalies_detection_timestamp ON anomalies(detection_timestamp);
        CREATE INDEX IF NOT EXISTS idx_anomalies_created_at ON anomalies(created_at);
        """
        
        async with self.db_pool.acquire() as conn:
            await conn.execute(create_anomalies_table)
            logger.info("Anomaly tables created/verified")
    
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
            retries=3
        )
        
        logger.info("Kafka consumer and producer initialized")
    
    def detect_login_anomalies(self, signal: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect login-related anomalies."""
        anomalies = []
        user_id = signal['user_id']
        timestamp = signal['timestamp']
        payload = signal['payload']
        
        # Track login attempts
        user_session = self.user_sessions[user_id]
        user_session['login_attempts'].append(timestamp)
        user_session['last_activity'] = timestamp
        
        # Remove old login attempts (older than 1 minute)
        cutoff_time = timestamp - (60 * 1000)  # 1 minute in milliseconds
        while user_session['login_attempts'] and user_session['login_attempts'][0] < cutoff_time:
            user_session['login_attempts'].popleft()
        
        # Rule 1: High frequency logins (> 10 per minute)
        if len(user_session['login_attempts']) > 10:
            anomalies.append({
                'type': 'high_frequency_login',
                'severity': 'high',
                'description': f'User {user_id} attempted {len(user_session["login_attempts"])} logins in the last minute',
                'user_id': user_id,
                'timestamp': timestamp,
                'original_signal': signal,
                'metadata': {
                    'login_count': len(user_session['login_attempts']),
                    'time_window': '1_minute'
                }
            })
        
        # Rule 2: Failed login patterns
        if not payload.get('success', True):
            # Check for repeated failures
            recent_failures = [ts for ts in user_session['login_attempts'][-5:]]  # Last 5 attempts
            if len(recent_failures) >= 3:
                anomalies.append({
                    'type': 'repeated_login_failures',
                    'severity': 'medium',
                    'description': f'User {user_id} has {len(recent_failures)} recent login failures',
                    'user_id': user_id,
                    'timestamp': timestamp,
                    'original_signal': signal,
                    'metadata': {
                        'failure_count': len(recent_failures),
                        'time_window': '1_minute'
                    }
                })
        
        # Rule 3: Unusual location (if different from recent pattern)
        location = payload.get('location')
        if location and hasattr(user_session, 'recent_locations'):
            if location not in user_session.get('recent_locations', []):
                anomalies.append({
                    'type': 'unusual_location_login',
                    'severity': 'medium',
                    'description': f'User {user_id} logged in from unusual location: {location}',
                    'user_id': user_id,
                    'timestamp': timestamp,
                    'original_signal': signal,
                    'metadata': {
                        'new_location': location,
                        'known_locations': user_session.get('recent_locations', [])
                    }
                })
        
        return anomalies
    
    def detect_purchase_anomalies(self, signal: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect purchase-related anomalies."""
        anomalies = []
        user_id = signal['user_id']
        timestamp = signal['timestamp']
        payload = signal['payload']
        amount = payload.get('amount', 0)
        
        # Track purchases
        user_session = self.user_sessions[user_id]
        user_session['recent_purchases'].append({'amount': amount, 'timestamp': timestamp})
        user_session['last_activity'] = timestamp
        
        # Remove old purchases (older than 1 hour)
        cutoff_time = timestamp - (60 * 60 * 1000)  # 1 hour in milliseconds
        while (user_session['recent_purchases'] and 
               user_session['recent_purchases'][0]['timestamp'] < cutoff_time):
            user_session['recent_purchases'].popleft()
        
        # Rule 1: High value purchase (> $1000)
        if amount > 1000:
            anomalies.append({
                'type': 'high_value_purchase',
                'severity': 'high',
                'description': f'User {user_id} made a high-value purchase of ${amount:.2f}',
                'user_id': user_id,
                'timestamp': timestamp,
                'original_signal': signal,
                'metadata': {
                    'amount': amount,
                    'threshold': 1000
                }
            })
        
        # Rule 2: Rapid successive purchases
        if len(user_session['recent_purchases']) >= 5:
            total_amount = sum(p['amount'] for p in user_session['recent_purchases'])
            anomalies.append({
                'type': 'rapid_successive_purchases',
                'severity': 'medium',
                'description': f'User {user_id} made {len(user_session["recent_purchases"])} purchases totaling ${total_amount:.2f} in the last hour',
                'user_id': user_id,
                'timestamp': timestamp,
                'original_signal': signal,
                'metadata': {
                    'purchase_count': len(user_session['recent_purchases']),
                    'total_amount': total_amount,
                    'time_window': '1_hour'
                }
            })
        
        return anomalies
    
    def detect_api_anomalies(self, signal: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect API call-related anomalies."""
        anomalies = []
        user_id = signal['user_id']
        timestamp = signal['timestamp']
        payload = signal['payload']
        status_code = payload.get('status_code', 200)
        
        # Track API failures
        user_session = self.user_sessions[user_id]
        if status_code >= 400:  # Error status codes
            user_session['api_failures'].append(timestamp)
        user_session['last_activity'] = timestamp
        
        # Remove old failures (older than 1 minute)
        cutoff_time = timestamp - (60 * 1000)  # 1 minute in milliseconds
        while user_session['api_failures'] and user_session['api_failures'][0] < cutoff_time:
            user_session['api_failures'].popleft()
        
        # Rule: High API failure rate (> 5 failures per minute)
        if len(user_session['api_failures']) > 5:
            anomalies.append({
                'type': 'high_api_failure_rate',
                'severity': 'high',
                'description': f'User {user_id} has {len(user_session["api_failures"])} API failures in the last minute',
                'user_id': user_id,
                'timestamp': timestamp,
                'original_signal': signal,
                'metadata': {
                    'failure_count': len(user_session['api_failures']),
                    'time_window': '1_minute',
                    'current_status_code': status_code
                }
            })
        
        return anomalies
    
    def detect_time_anomalies(self, signal: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect time-based anomalies."""
        anomalies = []
        user_id = signal['user_id']
        timestamp = signal['timestamp']
        
        # Convert timestamp to datetime
        dt = datetime.fromtimestamp(timestamp / 1000)
        hour = dt.hour
        
        # Rule: Unusual time activity (3AM hour)
        if hour == 3:
            anomalies.append({
                'type': 'unusual_time_activity',
                'severity': 'medium',
                'description': f'User {user_id} active during unusual hours (3AM)',
                'user_id': user_id,
                'timestamp': timestamp,
                'original_signal': signal,
                'metadata': {
                    'hour': hour,
                    'activity_type': signal['type']
                }
            })
        
        return anomalies
    
    async def process_signal(self, signal: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process a single signal and detect anomalies."""
        start_time = time.time()
        all_anomalies = []
        
        try:
            signal_type = signal.get('type')
            
            # Apply different detection rules based on signal type
            if signal_type == 'login':
                all_anomalies.extend(self.detect_login_anomalies(signal))
            elif signal_type == 'purchase':
                all_anomalies.extend(self.detect_purchase_anomalies(signal))
            elif signal_type == 'api_call':
                all_anomalies.extend(self.detect_api_anomalies(signal))
            
            # Always check time-based anomalies
            all_anomalies.extend(self.detect_time_anomalies(signal))
            
            # Record detection latency
            detection_latency.observe(time.time() - start_time)
            
            return all_anomalies
            
        except Exception as e:
            logger.error(f"Error processing signal: {e}")
            return []
    
    async def store_anomaly(self, anomaly: Dict[str, Any]):
        """Store anomaly in database."""
        try:
            insert_query = """
            INSERT INTO anomalies (user_id, anomaly_type, severity, description, 
                                 original_signal_data, detection_timestamp)
            VALUES ($1, $2, $3, $4, $5, $6)
            """
            
            async with self.db_pool.acquire() as conn:
                await conn.execute(
                    insert_query,
                    anomaly['user_id'],
                    anomaly['type'],
                    anomaly['severity'],
                    anomaly['description'],
                    json.dumps(anomaly['original_signal']),
                    anomaly['timestamp']
                )
            
            logger.info(f"Stored anomaly: {anomaly['type']} for user {anomaly['user_id']}")
            
        except Exception as e:
            logger.error(f"Error storing anomaly: {e}")
    
    async def publish_anomaly(self, anomaly: Dict[str, Any]):
        """Publish anomaly to Kafka topic."""
        try:
            # Prepare anomaly event for publishing
            anomaly_event = {
                'anomaly_id': f"{anomaly['user_id']}_{anomaly['timestamp']}_{anomaly['type']}",
                'user_id': anomaly['user_id'],
                'type': anomaly['type'],
                'severity': anomaly['severity'],
                'description': anomaly['description'],
                'timestamp': anomaly['timestamp'],
                'metadata': anomaly.get('metadata', {}),
                'detected_at': int(time.time() * 1000)
            }
            
            # Send to Kafka
            future = self.producer.send(
                self.output_topic,
                value=anomaly_event,
                key=anomaly['user_id']
            )
            
            # Update metrics
            anomalies_detected.labels(anomaly_type=anomaly['type']).inc()
            
            logger.info(f"Published anomaly: {anomaly['type']} for user {anomaly['user_id']}")
            
        except Exception as e:
            logger.error(f"Error publishing anomaly: {e}")
    
    def cleanup_old_sessions(self):
        """Remove old user sessions to prevent memory bloat."""
        current_time = time.time() * 1000  # Convert to milliseconds
        cutoff_time = current_time - (30 * 60 * 1000)  # 30 minutes
        
        users_to_remove = []
        for user_id, session in self.user_sessions.items():
            if session.get('last_activity', 0) < cutoff_time:
                users_to_remove.append(user_id)
        
        for user_id in users_to_remove:
            del self.user_sessions[user_id]
        
        if users_to_remove:
            logger.info(f"Cleaned up {len(users_to_remove)} inactive user sessions")
        
        # Update active sessions metric
        active_user_sessions.set(len(self.user_sessions))
    
    async def consume_and_detect(self):
        """Main consumer loop for anomaly detection with concurrent workers."""
        logger.info("Starting anomaly detection with concurrency...")

        queue: Queue = Queue(maxsize=2000)

        async def enqueue_signals():
            while True:
                message_pack = self.consumer.poll(timeout_ms=500, max_records=50)
                for _, messages in message_pack.items():
                    for message in messages:
                        await queue.put(message.value)
                await asyncio.sleep(0)

        async def worker(worker_id: int):
            while True:
                signal = await queue.get()
                try:
                    anomalies = await self.process_signal(signal)
                    # Fire off store and publish for each anomaly concurrently
                    for anomaly in anomalies:
                        await asyncio.gather(
                            self.store_anomaly(anomaly),
                            self.publish_anomaly(anomaly)
                        )
                except Exception as e:
                    logger.error(f"Worker {worker_id} error: {e}")
                finally:
                    queue.task_done()

        workers = [asyncio.create_task(worker(i)) for i in range(4)]
        producer_task = asyncio.create_task(enqueue_signals())

        try:
            while True:
                # Periodic cleanup
                current_time = time.time()
                if current_time - self.last_cleanup > self.cleanup_interval:
                    self.cleanup_old_sessions()
                    self.last_cleanup = current_time
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.info("Anomaly detection cancelled")
        except Exception as e:
            logger.error(f"Error in anomaly detection loop: {e}")
            raise
        finally:
            producer_task.cancel()
            for w in workers:
                w.cancel()
            await queue.join()
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.close()

# FastAPI app for health checks and metrics
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
        "active_sessions": len(detector.user_sessions) if detector else 0
    }

@app.get("/metrics/custom")
async def custom_metrics():
    """Custom application metrics."""
    if not detector or not detector.db_pool:
        return {"error": "Database not initialized"}
    
    try:
        async with detector.db_pool.acquire() as conn:
            # Get anomaly statistics
            stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_anomalies,
                    COUNT(CASE WHEN severity = 'high' THEN 1 END) as high_severity,
                    COUNT(CASE WHEN severity = 'medium' THEN 1 END) as medium_severity,
                    COUNT(CASE WHEN severity = 'low' THEN 1 END) as low_severity
                FROM anomalies 
                WHERE created_at > NOW() - INTERVAL '1 hour'
            """)
            
            # Get anomaly counts by type
            anomaly_types = await conn.fetch("""
                SELECT anomaly_type, COUNT(*) as count
                FROM anomalies 
                WHERE created_at > NOW() - INTERVAL '1 hour'
                GROUP BY anomaly_type
                ORDER BY count DESC
            """)
            
            return {
                "total_anomalies_last_hour": stats['total_anomalies'] or 0,
                "high_severity_anomalies": stats['high_severity'] or 0,
                "medium_severity_anomalies": stats['medium_severity'] or 0,
                "low_severity_anomalies": stats['low_severity'] or 0,
                "anomaly_types": [dict(row) for row in anomaly_types],
                "active_user_sessions": len(detector.user_sessions) if detector else 0
            }
    except Exception as e:
        logger.error(f"Error getting custom metrics: {e}")
        return {"error": str(e)}

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