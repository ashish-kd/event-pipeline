import asyncio
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict

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

# Prometheus metrics per specification
signals_produced_total = Counter('signals_produced_total', 'Total signals produced')
produce_errors_total = Counter('produce_errors_total', 'Total produce errors')
produce_latency_seconds = Histogram('produce_latency_seconds', 'Produce latency in seconds')

def generate_event_id() -> str:
    """Generate time-ordered UUID (using uuid1 for Python 3.11)."""
    return str(uuid.uuid1())

class SignalGenerator:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.signals_per_second = int(os.getenv('SIGNALS_PER_SECOND', '1000'))
        self.topic = 'signal-events'
        
        # Signal types and sources
        self.signal_types = ['login', 'purchase', 'click', 'api_call', 'file_access']
        self.sources = ['web', 'mobile', 'api']
        
        # Initialize Kafka producer per specification
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            acks='all',
            retries=5  # Per specification
        )
        
        logger.info(f"Signal generator initialized - {self.signals_per_second} signals/sec")
        
    def generate_signal(self) -> Dict:
        """Generate signal with structure per specification."""
        now = datetime.now(timezone.utc)
        
        signal = {
            'event_id': generate_event_id(),
            'user_id': f"user_{random.randint(1, 1000)}",
            'source': random.choice(self.sources),
            'type': random.choice(self.signal_types),
            'event_ts': now.isoformat(),
            'ingest_ts': now.isoformat(),
            'payload': self._generate_payload()
        }
        
        return signal
    
    def _generate_payload(self) -> Dict:
        """Generate minimal payload."""
        return {
            'session_id': f"sess_{random.randint(10000, 99999)}",
            'data': f"sample_data_{random.randint(1, 100)}"
        }
    
    async def produce_signals(self):
        """Main signal production loop."""
        logger.info("Starting signal production...")
        
        interval = 1.0 / max(self.signals_per_second, 1)
        
        while True:
            start_time = time.time()
            
            try:
                with produce_latency_seconds.time():
                    signal = self.generate_signal()
                    
                    # Send to Kafka
                    future = self.producer.send(
                        self.topic, 
                        value=signal, 
                        key=signal['user_id']
                    )
                    
                    signals_produced_total.inc()
                    
            except Exception as e:
                produce_errors_total.inc()
                logger.error(f"Error producing signal: {e}")
            
            # Rate limiting
            elapsed = time.time() - start_time
            sleep_time = max(0.0, interval - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

# FastAPI app for testing and health checks
app = FastAPI(title="Signal Emitter Service", version="1.0.0")

# Global generator instance
generator = None

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "signal-emitter"}

@app.post("/emit")
async def emit_signal():
    """Manual signal emission for testing."""
    global generator
    if not generator:
        return {"status": "error", "message": "Generator not initialized"}
    
    try:
        with produce_latency_seconds.time():
            signal = generator.generate_signal()
            
            # Send to Kafka
            future = generator.producer.send(
                generator.topic, 
                value=signal, 
                key=signal['user_id']
            )
            
            signals_produced_total.inc()
            
            return {
                "status": "success", 
                "signal_id": signal['event_id'],
                "user_id": signal['user_id'],
                "type": signal['type']
            }
        
    except Exception as e:
        produce_errors_total.inc()
        return {"status": "error", "message": str(e)}

@app.on_event("startup")
async def startup_event():
    """Start the signal generator and Prometheus metrics server."""
    global generator
    
    # Start Prometheus metrics server
    start_http_server(8001)
    logger.info("Prometheus metrics server started on port 8001")
    
    # Initialize global generator
    generator = SignalGenerator()
    
    # Start signal generation
    asyncio.create_task(generator.produce_signals())

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)