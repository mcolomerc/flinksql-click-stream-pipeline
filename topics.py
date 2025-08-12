#!/usr/bin/env python3
"""
Kafka Topics and Schema Management for Pipeline
"""

import json
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from config import config

# AVRO Schema for Click Events
CLICK_EVENT_SCHEMA = {
    "type": "record",
    "name": "ClickEvent",
    "namespace": "com.pipeline.events",
    "fields": [
        {"name": "eventTime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
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

# AVRO Schema for Enriched Events (output)
ENRICHED_EVENT_SCHEMA = {
    "type": "record",
    "name": "EnrichedClickEvent", 
    "namespace": "com.pipeline.events",
    "fields": [
        {"name": "eventTime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
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

def register_schemas():
    """Register AVRO schemas with Schema Registry"""
    print("🔧 Registering schemas with Schema Registry...")
    
    if not config.validate_config():
        return False
    
    try:
        # Create schema registry client
        sr_config = config.get_schema_registry_config()
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
        output_schema_str = json.dumps(ENRICHED_EVENT_SCHEMA)
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

def create_topics():
    """Create required Kafka topics"""
    print("🔧 Creating Kafka topics...")
    
    if not config.validate_config():
        return False
    
    # Create admin client
    admin_conf = config.get_kafka_config()
    admin_client = AdminClient(admin_conf)
    
    topic_names = config.get_topic_names()
    
    # Define topics
    topics = [
        NewTopic(
            topic=topic_names['input'],
            num_partitions=3,
            replication_factor=3,
            config={
                'cleanup.policy': 'delete',
                'retention.ms': '86400000'  # 24 hours
            }
        ),
        NewTopic(
            topic=topic_names['output'], 
            num_partitions=3,
            replication_factor=3,
            config={
                'cleanup.policy': 'delete',
                'retention.ms': '86400000'  # 24 hours
            }
        )
    ]
    
    # Create topics
    futures = admin_client.create_topics(topics)
    
    # Wait for topic creation
    for topic_name, future in futures.items():
        try:
            future.result(timeout=30)
            print(f"✅ Created topic: {topic_name}")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"ℹ️  Topic already exists: {topic_name}")
            else:
                print(f"❌ Failed to create topic {topic_name}: {e}")
                return False
    
    print("✅ Topics created successfully")
    
    # Register schemas after topics are created
    return register_schemas()

def delete_topics():
    """Delete pipeline topics and schemas"""
    print("🗑️  Deleting Kafka topics and schemas...")
    
    admin_conf = config.get_kafka_config()
    admin_client = AdminClient(admin_conf)
    
    topic_names = config.get_topic_names()
    topics_to_delete = list(topic_names.values())
    
    # Delete topics
    futures = admin_client.delete_topics(topics_to_delete)
    
    # Wait for deletion
    for topic_name, future in futures.items():
        try:
            future.result(timeout=30)
            print(f"✅ Deleted topic: {topic_name}")
        except Exception as e:
            if "does not exist" in str(e).lower():
                print(f"ℹ️  Topic does not exist: {topic_name}")
            else:
                print(f"❌ Failed to delete topic {topic_name}: {e}")
    
    # Clean up schemas
    try:
        sr_config = config.get_schema_registry_config()
        schema_client = SchemaRegistryClient(sr_config)
        
        for topic_name in topic_names.values():
            subject = f"{topic_name}-value"
            try:
                versions = schema_client.delete_subject(subject)
                print(f"✅ Deleted schema subject: {subject} (versions: {versions})")
            except Exception as e:
                if "not found" in str(e).lower():
                    print(f"ℹ️  Schema subject not found: {subject}")
                else:
                    print(f"❌ Failed to delete schema subject {subject}: {e}")
                    
    except Exception as e:
        print(f"⚠️  Error cleaning up schemas: {e}")

if __name__ == "__main__":
    create_topics()
