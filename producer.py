#!/usr/bin/env python3
"""
Click Stream Events Producer
Generates 10 events: 5 for user1, 5 for user2
Each user gets 1 search event + 4 product events
Uses AVRO serialization with Schema Registry
"""

import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from config import config

class ClickStreamProducer:
    def __init__(self):
        self.producer = None
        self.topic_name = None
        self.avro_serializer = None
        
    def setup(self):
        """Initialize Kafka producer with AVRO serialization"""
        if not config.validate_config():
            return False
        
        # Setup Schema Registry client
        sr_config = config.get_schema_registry_config()
        schema_client = SchemaRegistryClient(sr_config)
        
        # Get topic name
        topic_names = config.get_topic_names()
        self.topic_name = topic_names['input']
        
        # Define the schema (inline for now, will be registered by topics.py)
        schema_str = """{
            "type": "record",
            "name": "ClickEvent",
            "namespace": "com.pipeline.events",
            "fields": [
                {"name": "eventTime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "userId", "type": "string"},
                {"name": "clickId", "type": "string"},
                {"name": "eventType", "type": "string"},
                {"name": "searchId", "type": ["null", "string"], "default": null},
                {"name": "productId", "type": ["null", "string"], "default": null},
                {"name": "query", "type": ["null", "string"], "default": null},
                {"name": "referrer", "type": ["null", "string"], "default": null},
                {"name": "metadata", "type": {"type": "map", "values": "string"}}
            ]
        }"""
        
        try:
            # Create AVRO serializer with schema string directly
            # This way we don't depend on the schema being registered yet
            self.avro_serializer = AvroSerializer(
                schema_registry_client=schema_client,
                schema_str=schema_str
            )
            
            print(f"✅ Producer setup complete for topic: {self.topic_name}")
            
        except Exception as e:
            print(f"❌ Failed to setup producer: {e}")
            return False
        
        # Setup producer
        producer_conf = config.get_kafka_config()
        producer_conf.update({
            'client.id': f'click-producer-{config.pipeline_id}',
            'acks': 'all',
            'retries': 5,
            'enable.idempotence': True
        })
        
        self.producer = Producer(producer_conf)
        
        print(f"✅ Producer initialized for topic: {self.topic_name}")
        return True
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f"❌ Message delivery failed: {err}")
        else:
            print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
    
    def generate_events(self):
        """Generate and send click stream events"""
        print("🚀 Generating click stream events...")
        
        users = ['user1', 'user2']
        events = []
        
        for user_id in users:
            search_id = str(uuid.uuid4())
            base_time = datetime.now(timezone.utc)
            
            # 1. Search event (with searchId)
            search_event = {
                'eventTime': int(base_time.timestamp() * 1000),  # Convert to milliseconds
                'userId': user_id,
                'clickId': str(uuid.uuid4()),
                'eventType': 'search',
                'searchId': search_id,
                'productId': None,
                'query': f'search query for {user_id}',
                'referrer': 'google.com',
                'metadata': {
                    'browser': 'chrome',
                    'device': 'desktop'
                }
            }
            events.append(search_event)
            
            # 2. Four product events (without searchId - to be enriched)
            for i in range(4):
                # Add some delay between events
                event_time = datetime.fromtimestamp(
                    base_time.timestamp() + (i + 1) * 2,  # 2 second intervals
                    timezone.utc
                )
                
                product_event = {
                    'eventTime': int(event_time.timestamp() * 1000),  # Convert to milliseconds
                    'userId': user_id,
                    'clickId': str(uuid.uuid4()),
                    'eventType': 'product_click',
                    'searchId': None,  # This should be enriched by Flink
                    'productId': f'product_{i+1}',
                    'query': None,
                    'referrer': 'search_results',
                    'metadata': {
                        'browser': 'chrome',
                        'device': 'desktop'
                    }
                }
                events.append(product_event)
        
        # Send all events
        for i, event in enumerate(events):
            try:
                message_key = event['userId']
                
                # Serialize value using AVRO
                serialized_value = self.avro_serializer(
                    event, 
                    SerializationContext(self.topic_name, MessageField.VALUE)
                )
                
                print(f"📤 Sending event {i+1}/10: {event['eventType']} for {event['userId']}")
                
                self.producer.produce(
                    topic=self.topic_name,
                    key=message_key,
                    value=serialized_value,
                    callback=self.delivery_report
                )
                
                # Small delay between messages
                import time
                time.sleep(1)
                
            except Exception as e:
                print(f"❌ Failed to send event: {e}")
                return False
        
        # Wait for all messages to be delivered
        print("⏳ Waiting for message delivery...")
        self.producer.flush(timeout=30)
        
        print("✅ All events sent successfully!")
        return True
    
    def close(self):
        """Close producer"""
        if self.producer:
            self.producer.flush()

def main():
    """Main function to run the producer"""
    producer = ClickStreamProducer()
    
    if not producer.setup():
        print("❌ Failed to setup producer")
        return False
    
    try:
        success = producer.generate_events()
        return success
    finally:
        producer.close()

if __name__ == "__main__":
    main()
