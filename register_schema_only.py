#!/usr/bin/env python3
"""
Just register schemas without creating topics
"""

import json
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from config import config

# AVRO Schema for Click Events
CLICK_EVENT_SCHEMA = {
    "type": "record",
    "name": "ClickEvent",
    "namespace": "com.pipeline.events",
    "fields": [
        {"name": "eventTime", "type": "string"},
        {"name": "userId", "type": "string"},
        {"name": "clickId", "type": "string"},
        {"name": "eventType", "type": "string"},
        {"name": "searchId", "type": ["null", "string"], "default": None},
        {"name": "productId", "type": ["null", "string"], "default": None},
        {"name": "query", "type": ["null", "string"], "default": None},
        {"name": "referrer", "type": ["null", "string"], "default": None},
        {"name": "metadata", "type": {"type": "map", "values": "string"}}
    ]
}

def register_schemas_only():
    """Register AVRO schemas with Schema Registry only"""
    print("🔧 Registering schemas with Schema Registry...")
    
    if not config.validate_config():
        return False
    
    try:
        # Create schema registry client
        sr_config = config.get_schema_registry_config()
        print(f"🔗 Connecting to Schema Registry: {sr_config['url']}")
        schema_client = SchemaRegistryClient(sr_config)
        
        topic_names = config.get_topic_names()
        
        # Register input topic schema (value)
        input_schema_str = json.dumps(CLICK_EVENT_SCHEMA)
        input_subject = f"{topic_names['input']}-value"
        
        print(f"📝 Registering schema for subject: {input_subject}")
        input_schema = Schema(input_schema_str, schema_type="AVRO")
        input_schema_id = schema_client.register_schema(
            input_subject,
            input_schema
        )
        print(f"✅ Input schema registered with ID: {input_schema_id}")
        
        # Register output topic schema (value) 
        output_schema_str = json.dumps({
            "type": "record",
            "name": "EnrichedClickEvent", 
            "namespace": "com.pipeline.events",
            "fields": [
                {"name": "eventTime", "type": "string"},
                {"name": "userId", "type": "string"},
                {"name": "clickId", "type": "string"},
                {"name": "eventType", "type": "string"},
                {"name": "searchId", "type": ["null", "string"], "default": None},
                {"name": "productId", "type": ["null", "string"], "default": None},
                {"name": "query", "type": ["null", "string"], "default": None},
                {"name": "referrer", "type": ["null", "string"], "default": None},
                {"name": "metadata", "type": {"type": "map", "values": "string"}}
            ]
        })
        output_subject = f"{topic_names['output']}-value"
        
        print(f"📝 Registering schema for subject: {output_subject}")
        output_schema = Schema(output_schema_str, schema_type="AVRO")
        output_schema_id = schema_client.register_schema(
            output_subject,
            output_schema
        )
        print(f"✅ Output schema registered with ID: {output_schema_id}")
        
        print("✅ All schemas registered successfully")
        return True
        
    except Exception as e:
        print(f"❌ Failed to register schemas: {e}")
        return False

if __name__ == "__main__":
    register_schemas_only()
