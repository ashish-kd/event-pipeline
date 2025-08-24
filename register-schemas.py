#!/usr/bin/env python3
"""Register Avro schemas with Schema Registry."""

import json
import os
import requests
import time
import logging
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SCHEMA_REGISTRY_URL = os.getenv('SCHEMA_REGISTRY_URL', 'http://localhost:8081')

def wait_for_schema_registry(max_retries=30):
    """Wait for Schema Registry to be ready."""
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
            if response.status_code == 200:
                logger.info("Schema Registry is ready")
                return True
        except requests.exceptions.ConnectionError:
            pass
        
        logger.info(f"Waiting for Schema Registry... attempt {attempt + 1}/{max_retries}")
        time.sleep(2)
    
    logger.error("Schema Registry not available after maximum retries")
    return False

def register_schema(subject, schema_file):
    """Register a schema for a subject."""
    try:
        with open(schema_file, 'r') as f:
            schema_content = json.load(f)
        
        # Prepare the payload for Schema Registry
        payload = {
            "schema": json.dumps(schema_content)
        }
        
        headers = {'Content-Type': 'application/vnd.schemaregistry.v1+json'}
        
        response = requests.post(
            f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions",
            json=payload,
            headers=headers
        )
        
        if response.status_code in [200, 409]:  # 409 = already exists
            result = response.json()
            logger.info(f"Schema registered for {subject}: version {result.get('id', 'existing')}")
            return True
        else:
            logger.error(f"Failed to register schema for {subject}: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error registering schema for {subject}: {e}")
        return False

def set_global_compatibility():
    """Set global compatibility level to BACKWARD for safe evolution."""
    try:
        response = requests.put(
            f"{SCHEMA_REGISTRY_URL}/config",
            json={"compatibility": "BACKWARD"},
            headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'}
        )
        
        if response.status_code == 200:
            logger.info("✅ Global compatibility set to BACKWARD")
            return True
        else:
            logger.error(f"❌ Failed to set global compatibility: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Error setting global compatibility: {e}")
        return False

def set_subject_compatibility(subject, compatibility_level="BACKWARD"):
    """Set compatibility level for specific subject."""
    try:
        response = requests.put(
            f"{SCHEMA_REGISTRY_URL}/config/{subject}",
            json={"compatibility": compatibility_level},
            headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'}
        )
        
        if response.status_code == 200:
            logger.info(f"✅ Subject {subject} compatibility set to {compatibility_level}")
            return True
        else:
            logger.error(f"❌ Failed to set {subject} compatibility: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        logger.error(f"❌ Error setting {subject} compatibility: {e}")
        return False

def main():
    """Register all schemas and configure governance."""
    if not wait_for_schema_registry():
        sys.exit(1)
    
    # 1. Configure schema governance first
    logger.info("🔧 Configuring Schema Registry Governance")
    governance_success = True
    
    if not set_global_compatibility():
        governance_success = False
    
    # 2. Register schemas
    schemas_to_register = [
        ("signal-events-value", "schemas/signal.avsc"),
        ("anomaly-events-value", "schemas/anomaly.avsc")
    ]
    
    success = True
    for subject, schema_file in schemas_to_register:
        if not register_schema(subject, schema_file):
            success = False
            
        # Set subject-specific compatibility after registration
        if not set_subject_compatibility(subject, "BACKWARD"):
            governance_success = False
    
    if success and governance_success:
        logger.info("All schemas registered successfully with governance configured")
        logger.info("✅ Compatibility enforcement: BACKWARD")
        logger.info("✅ Evolution policy: Safe backward-compatible changes only")
        
        # List registered schemas
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
        if response.status_code == 200:
            subjects = response.json()
            logger.info(f"Registered subjects: {subjects}")
    else:
        logger.error("Some schemas failed to register or governance configuration failed")
        sys.exit(1)

if __name__ == '__main__':
    main()
