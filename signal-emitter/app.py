import asyncio
import json
import logging
import os
import random
import time
from asyncio import Queue, Semaphore
from datetime import datetime
from typing import Dict, List

from kafka import KafkaProducer
from prometheus_client import Counter, Histogram, start_http_server
import uvicorn
from fastapi import FastAPI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
signals_produced = Counter('signals_produced_total', 'Total signals produced', ['signal_type'])
signal_latency = Histogram('signal_production_latency_seconds', 'Signal production latency')

class SignalGenerator:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.signals_per_second = int(os.getenv('SIGNALS_PER_SECOND', '1000'))
        self.burst_mode = os.getenv('BURST_MODE', 'false').lower() == 'true'
        self.worker_count = int(os.getenv('EMITTER_WORKERS', '4'))
        self.queue_maxsize = int(os.getenv('EMITTER_QUEUE_MAXSIZE', '10000'))
        self.flush_every = int(os.getenv('EMITTER_FLUSH_EVERY', '1000'))
        self.topic = 'signal-events'
        
        # User behavior patterns
        self.user_ids = list(range(1, 1001))  # 1000 users
        self.signal_types = ['login', 'purchase', 'click', 'api_call', 'file_access']
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=1,
            buffer_memory=33554432
        )
        
        logger.info(f"Signal generator initialized - {self.signals_per_second} signals/sec")
        
    def generate_signal(self) -> Dict:
        """Generate a realistic signal based on user behavior patterns."""
        user_id = random.choice(self.user_ids)
        signal_type = self._get_weighted_signal_type()
        timestamp = int(time.time() * 1000)  # milliseconds
        
        payload = self._generate_payload(signal_type, user_id)
        
        signal = {
            'user_id': user_id,
            'type': signal_type,
            'timestamp': timestamp,
            'payload': payload
        }
        
        return signal
    
    def _get_weighted_signal_type(self) -> str:
        """Get signal type with realistic frequency distribution."""
        weights = {
            'click': 0.4,
            'api_call': 0.3,
            'login': 0.1,
            'file_access': 0.15,
            'purchase': 0.05
        }
        
        return random.choices(
            list(weights.keys()),
            weights=list(weights.values())
        )[0]
    
    def _generate_payload(self, signal_type: str, user_id: int) -> Dict:
        """Generate realistic payload based on signal type."""
        base_payload = {
            'session_id': f"sess_{user_id}_{random.randint(1000, 9999)}",
            'ip_address': f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            'user_agent': 'Mozilla/5.0 (compatible; EventPipeline/1.0)'
        }
        
        if signal_type == 'login':
            base_payload.update({
                'login_method': random.choice(['password', 'oauth', 'sso']),
                'success': random.choice([True, True, True, True, False]),  # 80% success rate
                'location': random.choice(['US', 'EU', 'ASIA', 'CA'])
            })
            
        elif signal_type == 'purchase':
            base_payload.update({
                'amount': round(random.uniform(5.0, 2000.0), 2),
                'currency': 'USD',
                'product_id': f"prod_{random.randint(100, 999)}",
                'payment_method': random.choice(['credit_card', 'paypal', 'bank_transfer'])
            })
            
        elif signal_type == 'click':
            base_payload.update({
                'page': random.choice(['/home', '/products', '/cart', '/profile', '/settings']),
                'element': random.choice(['button', 'link', 'image', 'form']),
                'coordinates': {'x': random.randint(0, 1920), 'y': random.randint(0, 1080)}
            })
            
        elif signal_type == 'api_call':
            base_payload.update({
                'endpoint': random.choice(['/api/users', '/api/products', '/api/orders', '/api/analytics']),
                'method': random.choice(['GET', 'POST', 'PUT', 'DELETE']),
                'status_code': random.choice([200, 200, 200, 400, 401, 500]),  # Mostly successful
                'response_time_ms': random.randint(10, 500)
            })
            
        elif signal_type == 'file_access':
            base_payload.update({
                'file_path': f"/data/user_{user_id}/file_{random.randint(1, 100)}.txt",
                'action': random.choice(['read', 'write', 'delete']),
                'file_size': random.randint(1024, 1048576)  # 1KB to 1MB
            })
        
        return base_payload
    
    def _add_anomaly_patterns(self, signal: Dict) -> Dict:
        """Occasionally inject anomaly patterns for testing detection."""
        if random.random() < 0.01:  # 1% chance of anomaly
            if signal['type'] == 'login':
                # High frequency login pattern
                signal['payload']['anomaly_trigger'] = 'high_frequency'
            elif signal['type'] == 'purchase':
                # High value purchase
                signal['payload']['amount'] = random.uniform(1500.0, 5000.0)
                signal['payload']['anomaly_trigger'] = 'high_value'
            elif signal['type'] == 'api_call':
                # Failed API calls
                signal['payload']['status_code'] = random.choice([401, 403, 500])
                signal['payload']['anomaly_trigger'] = 'failed_request'
        
        # Night activity (3AM pattern)
        current_hour = datetime.now().hour
        if current_hour == 3 and random.random() < 0.1:  # 10% chance during 3AM
            signal['payload']['anomaly_trigger'] = 'unusual_time'
        
        return signal
    
    async def _enqueue_signals(self, queue: Queue):
        """Rate-controlled producer that enqueues generated signals."""
        interval = 1.0 / max(self.signals_per_second, 1)
        batch_size = max(1, min(1000, self.signals_per_second // 10 or 1))
        logger.info(f"Signal enqueue loop started: interval={interval:.6f}s batch_size={batch_size}")

        while True:
            start_time = time.time()
            for _ in range(batch_size):
                with signal_latency.time():
                    signal = self.generate_signal()
                    signal = self._add_anomaly_patterns(signal)
                    await queue.put(signal)
            # Rate control to maintain target throughput
            elapsed = time.time() - start_time
            sleep_time = max(0.0, (batch_size * interval) - elapsed)
            if sleep_time:
                await asyncio.sleep(sleep_time)

            # Optional burst mode
            if self.burst_mode and random.random() < 0.1:
                for _ in range(batch_size * 5):
                    signal = self.generate_signal()
                    await queue.put(signal)

    async def _send_worker(self, queue: Queue, worker_id: int, flush_every: int):
        """Worker that sends signals from queue to Kafka concurrently."""
        sent_since_flush = 0
        while True:
            signal = await queue.get()
            try:
                # Non-blocking send; KafkaProducer buffers internally
                self.producer.send(self.topic, value=signal, key=signal['user_id'])
                signals_produced.labels(signal_type=signal['type']).inc()
                sent_since_flush += 1
                if sent_since_flush >= flush_every:
                    self.producer.flush()
                    sent_since_flush = 0
            except Exception as exc:
                logger.error(f"Worker {worker_id} failed to send signal: {exc}")
            finally:
                queue.task_done()

    async def produce_signals(self):
        """Main concurrent signal production pipeline."""
        logger.info(
            f"Starting signal production with workers={self.worker_count}, queue_maxsize={self.queue_maxsize}"
        )

        queue: Queue = Queue(maxsize=self.queue_maxsize)
        workers = [asyncio.create_task(self._send_worker(queue, i, self.flush_every)) for i in range(self.worker_count)]
        enqueue_task = asyncio.create_task(self._enqueue_signals(queue))

        try:
            await asyncio.gather(enqueue_task, *workers)
        except asyncio.CancelledError:
            logger.info("Signal production cancelled")
        except Exception as e:
            logger.error(f"Error in signal production: {e}")
            raise
        finally:
            # Drain pending messages and flush
            await queue.join()
            self.producer.flush()
            self.producer.close()

# FastAPI app for health checks and metrics
app = FastAPI(title="Signal Emitter Service", version="1.0.0")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "signal-emitter"}

@app.get("/metrics/custom")
async def custom_metrics():
    """Custom application metrics."""
    return {
        "signals_per_second": int(os.getenv('SIGNALS_PER_SECOND', '1000')),
        "burst_mode": os.getenv('BURST_MODE', 'false').lower() == 'true',
        "kafka_servers": os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    }

@app.on_event("startup")
async def startup_event():
    """Start the signal generator and Prometheus metrics server."""
    # Start Prometheus metrics server
    start_http_server(8001)
    logger.info("Prometheus metrics server started on port 8001")
    
    # Start signal generation
    generator = SignalGenerator()
    asyncio.create_task(generator.produce_signals())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 