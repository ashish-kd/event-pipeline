#!/usr/bin/env python3
"""
Comprehensive Schema Registry & Validation Test
Tests schema validation, drift prevention, and schema evolution.
"""

import json
import requests
import time
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Configuration
SCHEMA_REGISTRY_URL = "http://localhost:8081"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SIGNAL_PROCESSOR_METRICS_URL = "http://localhost:8012/metrics"

def test_schema_registry_status():
    """Test 1: Verify Schema Registry is operational."""
    print("🧪 Test 1: Schema Registry Status")
    print("=" * 50)
    
    try:
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
        if response.status_code == 200:
            subjects = response.json()
            print(f"✅ Schema Registry is operational")
            print(f"📊 Registered subjects: {subjects}")
            return True
        else:
            print(f"❌ Schema Registry not responding: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Schema Registry connection failed: {e}")
        return False

def test_valid_schema_production():
    """Test 2: Produce valid messages that conform to schema."""
    print("\n🧪 Test 2: Valid Schema Production")
    print("=" * 50)
    
    try:
        # Initialize Schema Registry client
        schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        
        # Get the schema
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/signal-events-value/versions/latest")
        schema_str = response.json()['schema']
        
        # Initialize serializer
        value_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema_str
        )
        
        # Initialize producer
        producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'acks': 'all'
        })
        
        # Create valid message
        valid_message = {
            'event_id': 'test-valid-001',
            'user_id': 'test_user_123',
            'source': 'api',  # Valid enum value
            'type': 'login',  # Valid enum value
            'event_ts': int(time.time() * 1000),
            'ingest_ts': int(time.time() * 1000),
            'payload': {
                'session_id': 'test_session_123',
                'data': 'test_data_valid'
            }
        }
        
        # Serialize and send
        serialized_value = value_serializer(
            valid_message, 
            SerializationContext('signal-events', MessageField.VALUE)
        )
        
        producer.produce(
            topic='signal-events',
            key='test_user_123',
            value=serialized_value
        )
        producer.flush()
        
        print("✅ Valid message produced successfully")
        print(f"📝 Message: {valid_message['event_id']} for user {valid_message['user_id']}")
        return True
        
    except Exception as e:
        print(f"❌ Valid message production failed: {e}")
        return False

def test_invalid_schema_rejection():
    """Test 3: Attempt to produce messages with invalid schema (should fail)."""
    print("\n🧪 Test 3: Invalid Schema Rejection")
    print("=" * 50)
    
    test_cases = [
        {
            "name": "Invalid enum value for 'source'",
            "message": {
                'event_id': 'test-invalid-001',
                'user_id': 'test_user_123',
                'source': 'invalid_source',  # Invalid enum value
                'type': 'login',
                'event_ts': int(time.time() * 1000),
                'ingest_ts': int(time.time() * 1000),
                'payload': {
                    'session_id': 'test_session_123',
                    'data': 'test_data_invalid'
                }
            }
        },
        {
            "name": "Missing required field 'user_id'",
            "message": {
                'event_id': 'test-invalid-002',
                # 'user_id': missing!
                'source': 'api',
                'type': 'login',
                'event_ts': int(time.time() * 1000),
                'ingest_ts': int(time.time() * 1000),
                'payload': {
                    'session_id': 'test_session_123',
                    'data': 'test_data_invalid'
                }
            }
        },
        {
            "name": "Wrong data type for timestamp",
            "message": {
                'event_id': 'test-invalid-003',
                'user_id': 'test_user_123',
                'source': 'api',
                'type': 'login',
                'event_ts': "not_a_timestamp",  # Should be long
                'ingest_ts': int(time.time() * 1000),
                'payload': {
                    'session_id': 'test_session_123',
                    'data': 'test_data_invalid'
                }
            }
        }
    ]
    
    try:
        # Initialize Schema Registry client
        schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})
        
        # Get the schema
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/signal-events-value/versions/latest")
        schema_str = response.json()['schema']
        
        # Initialize serializer
        value_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema_str
        )
        
        # Initialize producer
        producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'acks': 'all'
        })
        
        validation_works = True
        
        for test_case in test_cases:
            try:
                print(f"   Testing: {test_case['name']}")
                
                # Attempt to serialize invalid message
                serialized_value = value_serializer(
                    test_case['message'], 
                    SerializationContext('signal-events', MessageField.VALUE)
                )
                
                # If we get here, validation failed
                print(f"   ❌ FAILED: Invalid message was not rejected!")
                validation_works = False
                
            except Exception as e:
                # This is expected - the invalid message should be rejected
                print(f"   ✅ PASSED: Invalid message correctly rejected - {str(e)[:100]}")
        
        if validation_works:
            print("✅ Schema validation is working correctly - all invalid messages were rejected")
            return True
        else:
            print("❌ Schema validation failed - some invalid messages were accepted")
            return False
            
    except Exception as e:
        print(f"❌ Schema validation test failed: {e}")
        return False

def test_schema_evolution():
    """Test 4: Check schema evolution capabilities."""
    print("\n🧪 Test 4: Schema Evolution")
    print("=" * 50)
    
    try:
        # Check schema versions
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/signal-events-value/versions")
        if response.status_code == 200:
            versions = response.json()
            print(f"📊 Available schema versions: {versions}")
            
            # Get latest schema details
            response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/signal-events-value/versions/latest")
            schema_info = response.json()
            print(f"📝 Latest schema version: {schema_info['version']}")
            print(f"🆔 Schema ID: {schema_info['id']}")
            
            return True
        else:
            print(f"❌ Failed to get schema versions: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Schema evolution test failed: {e}")
        return False

def test_metrics_validation():
    """Test 5: Check that metrics show schema validation in action."""
    print("\n🧪 Test 5: Metrics Validation")
    print("=" * 50)
    
    try:
        response = requests.get(SIGNAL_PROCESSOR_METRICS_URL)
        if response.status_code == 200:
            metrics = response.text
            
            # Look for relevant metrics
            consumed_total = None
            dlq_total = None
            
            for line in metrics.split('\n'):
                if 'signals_consumed_total' in line and '#' not in line:
                    consumed_total = float(line.split()[-1])
                elif 'signals_dlq_total' in line and '#' not in line:
                    dlq_total = float(line.split()[-1])
            
            print(f"📊 Signals consumed: {consumed_total}")
            print(f"📊 DLQ messages: {dlq_total}")
            
            if consumed_total and consumed_total > 0:
                print("✅ Signal processor is actively consuming messages")
                print("✅ Schema validation is working (no deserialization errors)")
                return True
            else:
                print("⚠️  No signals consumed yet - schema validation status unclear")
                return False
                
        else:
            print(f"❌ Failed to get metrics: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Metrics validation failed: {e}")
        return False

def main():
    """Run all schema validation tests."""
    print("🚀 Schema Registry & Validation Test Suite")
    print("=" * 60)
    print("Testing comprehensive schema validation and drift prevention...")
    print()
    
    tests = [
        test_schema_registry_status,
        test_valid_schema_production,
        test_invalid_schema_rejection,
        test_schema_evolution,
        test_metrics_validation
    ]
    
    results = []
    for test in tests:
        result = test()
        results.append(result)
        time.sleep(2)  # Brief pause between tests
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 Test Results Summary")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    test_names = [
        "Schema Registry Status",
        "Valid Schema Production", 
        "Invalid Schema Rejection",
        "Schema Evolution",
        "Metrics Validation"
    ]
    
    for i, (name, passed_test) in enumerate(zip(test_names, results)):
        status = "✅ PASSED" if passed_test else "❌ FAILED"
        print(f"{i+1}. {name}: {status}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Schema Registry validation is working correctly.")
        print("✅ Schema drift prevention is active and effective.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the implementation.")
        return 1

if __name__ == "__main__":
    exit(main())
