import asyncio
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Dict

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import Counter, Histogram, start_http_server
import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Header
from typing import Annotated
import requests

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
        self.schema_registry_url = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')
        self.signals_per_second = int(os.getenv('SIGNALS_PER_SECOND', '1000'))
        self.topic = 'signal-events'
        
        # Signal types and sources (must match Avro enum)
        self.signal_types = ['login', 'purchase', 'click', 'api_call', 'file_access']
        self.sources = ['web', 'mobile', 'api']
        
        # Initialize Schema Registry client and serializer
        self.schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})
        self.value_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=self._load_schema_str()
        )
        
        # Initialize regular Kafka producer
        self.producer = Producer({
            'bootstrap.servers': self.kafka_servers,
            'acks': 'all',
            'retries': 5  # Per specification
        })
        
        logger.info(f"Signal generator initialized - {self.signals_per_second} signals/sec")
    
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
        
    def generate_signal(self) -> Dict:
        """Generate signal with Avro-compatible structure."""
        now = datetime.now(timezone.utc)
        timestamp_ms = int(now.timestamp() * 1000)  # Convert to milliseconds
        
        signal = {
            'event_id': generate_event_id(),
            'user_id': f"user_{random.randint(1, 1000)}",
            'source': random.choice(self.sources),
            'type': random.choice(self.signal_types),
            'event_ts': timestamp_ms,
            'ingest_ts': timestamp_ms,
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
                    
                    # Send to Kafka with Avro serialization
                    self.producer.produce(
                        topic=self.topic,
                        key=signal['user_id'],  # String key
                        value=self.value_serializer(signal, SerializationContext(self.topic, MessageField.VALUE))
                    )
                    
                    # Flush periodically to ensure delivery
                    self.producer.poll(0)
                    
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

# Simple API key authentication
def verify_api_key(x_api_key: Annotated[str | None, Header()] = None):
    """Basic API key verification for /emit endpoint."""
    valid_key = os.getenv('EMIT_API_KEY', 'dev-test-key-123')  # Default for dev
    if x_api_key != valid_key:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "signal-emitter"}

@app.post("/emit")
async def emit_signal(api_key: str = Depends(verify_api_key)):
    """Manual signal emission for testing."""
    global generator
    if not generator:
        return {"status": "error", "message": "Generator not initialized"}
    
    try:
        with produce_latency_seconds.time():
            signal = generator.generate_signal()
            
            # Send to Kafka with Avro serialization
            generator.producer.produce(
                topic=generator.topic,
                key=signal['user_id'],  # String key
                value=generator.value_serializer(signal, SerializationContext(generator.topic, MessageField.VALUE))
            )
            
            # Flush to ensure delivery
            generator.producer.flush()
            
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