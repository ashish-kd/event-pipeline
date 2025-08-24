#!/usr/bin/env python3
"""Test script to verify outbox pattern and transactional guarantees."""

import asyncio
import asyncpg
import json
import requests
import time
from datetime import datetime, timezone

# Configuration
DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/eventpipeline"
DEBEZIUM_URL = "http://localhost:8083"
EMIT_URL = "http://localhost:8001/emit"

async def test_outbox_pattern():
    """Test the outbox pattern implementation."""
    print("🧪 Testing Outbox Pattern & Transactional Guarantees")
    print("=" * 60)
    
    # 1. Check database connection
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Database connection: OK")
        await conn.close()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False
    
    # 2. Check Debezium connector status
    try:
        response = requests.get(f"{DEBEZIUM_URL}/connectors/outbox-connector/status")
        if response.status_code == 200:
            status = response.json()
            connector_state = status.get("connector", {}).get("state", "unknown")
            print(f"✅ Debezium connector: {connector_state}")
        else:
            print(f"❌ Debezium connector not responding: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Debezium connector check failed: {e}")
        return False
    
    # 3. Get baseline counts
    conn = await asyncpg.connect(DATABASE_URL)
    
    initial_anomalies = await conn.fetchval("SELECT COUNT(*) FROM anomalies")
    initial_outbox = await conn.fetchval("SELECT COUNT(*) FROM outbox_events")
    
    print(f"📊 Initial state: {initial_anomalies} anomalies, {initial_outbox} outbox events")
    
    # 4. Trigger signal emission to generate anomalies
    print("🚀 Emitting test signals...")
    
    # Emit signals with high data values to trigger anomalies
    for i in range(5):
        try:
            response = requests.post(EMIT_URL, headers={"X-API-Key": "dev-test-key-123"})
            if response.status_code == 200:
                print(f"   Signal {i+1}: emitted")
            else:
                print(f"   Signal {i+1}: failed ({response.status_code})")
        except Exception as e:
            print(f"   Signal {i+1}: error - {e}")
        
        time.sleep(0.5)  # Small delay between signals
    
    # 5. Wait for processing
    print("⏳ Waiting for anomaly detection and outbox processing...")
    await asyncio.sleep(10)
    
    # 6. Check results
    final_anomalies = await conn.fetchval("SELECT COUNT(*) FROM anomalies")
    final_outbox = await conn.fetchval("SELECT COUNT(*) FROM outbox_events")
    
    print(f"📊 Final state: {final_anomalies} anomalies, {final_outbox} outbox events")
    
    # 7. Verify atomicity - should have equal number of anomalies and outbox events
    new_anomalies = final_anomalies - initial_anomalies
    new_outbox = final_outbox - initial_outbox
    
    print(f"📈 New entries: {new_anomalies} anomalies, {new_outbox} outbox events")
    
    if new_anomalies == new_outbox and new_anomalies > 0:
        print("✅ ATOMICITY TEST PASSED: Equal anomalies and outbox events")
        atomic_test_passed = True
    else:
        print("❌ ATOMICITY TEST FAILED: Mismatch between anomalies and outbox events")
        atomic_test_passed = False
    
    # 8. Check recent outbox events
    recent_outbox = await conn.fetch("""
        SELECT event_type, aggregate_id, created_at, payload
        FROM outbox_events 
        ORDER BY created_at DESC 
        LIMIT 3
    """)
    
    if recent_outbox:
        print("📝 Recent outbox events:")
        for event in recent_outbox:
            payload = json.loads(event['payload'])
            print(f"   {event['event_type']}: {payload.get('anomaly_type')} (ID: {event['aggregate_id'][:8]}...)")
    
    await conn.close()
    
    # 9. Summary
    print("=" * 60)
    if atomic_test_passed and new_anomalies > 0:
        print("🎉 OUTBOX PATTERN TEST: PASSED")
        print("✅ Transactional guarantees are working correctly!")
        return True
    else:
        print("❌ OUTBOX PATTERN TEST: FAILED")
        print("⚠️  Transactional guarantees may not be working correctly")
        return False

async def main():
    """Main test runner."""
    success = await test_outbox_pattern()
    if success:
        print("\n🏆 All tests passed! Outbox pattern is working correctly.")
        return 0
    else:
        print("\n💥 Some tests failed. Check the implementation.")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
