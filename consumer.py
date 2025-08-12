#!/usr/bin/env python3
"""
Output Consumer
Consumes enriched events from the output topic using AVRO deserialization
"""

import json
import time
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from datetime import datetime
from config import config

class ClickStreamConsumer:
    def __init__(self):
        self.topic_name = f"output_{config.pipeline_id}"
        self.consumer = None
        self.avro_deserializer = None
        self.message_count = 0
        self.enriched_count = 0
        
    def setup(self):
        """Setup consumer and AVRO deserializer"""
        print(f"✅ Configuration validated successfully")
        
        # Schema Registry client
        schema_registry_conf = {
            'url': config.config['SCHEMA_REGISTRY_ENDPOINT'],
            'basic.auth.user.info': f"{config.config['SCHEMA_REGISTRY_API_KEY']}:{config.config['SCHEMA_REGISTRY_API_SECRET']}"
        }
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Use the subject name to get the registered schema instead of defining our own
        # This ensures we get the exact schema that was registered
        subject_name = f"{self.topic_name}-value"
        
        try:
            # Get the latest schema from Schema Registry
            latest_schema = schema_registry_client.get_latest_version(subject_name)
            print(f"📝 Retrieved schema ID {latest_schema.schema_id} for subject {subject_name}")
            
            # Create AVRO deserializer using the registered schema
            self.avro_deserializer = AvroDeserializer(
                schema_registry_client,
                latest_schema.schema.schema_str
            )
            
        except Exception as e:
            print(f"❌ Error retrieving schema: {e}")
            print("📝 Falling back to manual schema definition...")
            
            # Fallback: Define the schema manually to match what's actually in the registry
            schema_str = """{
                "type": "record",
                "name": "EnrichedClickEvent",
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
            
            self.avro_deserializer = AvroDeserializer(
                schema_registry_client,
                schema_str
            )
        
        # Consumer configuration
        consumer_conf = config.get_kafka_config()
        consumer_conf.update({
            'group.id': f'enriched-consumer-{config.pipeline_id}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'session.timeout.ms': 30000,
            'max.poll.interval.ms': 300000
        })
        
        self.consumer = Consumer(consumer_conf)
        
        print(f"✅ Consumer setup complete for topic: {self.topic_name}")
        print(f"✅ Consumer initialized for topic: {self.topic_name}")
        return True
    
    def consume_messages(self):
        """Consume and process enriched events"""
        print(f"🔍 Starting to consume from topic: {self.topic_name}")
        print("📋 Waiting for enriched events...")
        
        # Subscribe to topic
        self.consumer.subscribe([self.topic_name])
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Consumer error: {msg.error()}")
                        break
                
                try:
                    # Deserialize the message
                    event = self.avro_deserializer(
                        msg.value(),
                        SerializationContext(self.topic_name, MessageField.VALUE)
                    )
                    
                    self.message_count += 1
                    
                    # Convert timestamp from millis to readable format if it's a number
                    if isinstance(event['eventTime'], (int, float)):
                        event_time = datetime.fromtimestamp(event['eventTime'] / 1000)
                    else:
                        # Handle string timestamps if they exist
                        event_time = event['eventTime']
                    
                    # Check if it's an enriched event (not using fallback)
                    search_id = event.get('searchId', '')
                    is_enriched = (search_id and 
                                 not search_id.startswith('enriched-') and
                                 search_id != 'None')
                    
                    if is_enriched:
                        self.enriched_count += 1
                        print(f"✅ ENRICHED: {event['eventType']} by {event['userId']} at {event_time}")
                        print(f"   🔍 SearchId: {search_id}")
                        if event.get('productId'):
                            print(f"   🛍️  ProductId: {event['productId']}")
                    else:
                        print(f"⚠️  FALLBACK: {event['eventType']} by {event['userId']} at {event_time}")
                        print(f"   🔍 SearchId: {search_id}")
                    
                    print(f"   📝 ClickId: {event['clickId']}")
                    if event.get('query'):
                        print(f"   🔎 Query: {event['query']}")
                    print("---")
                    
                except Exception as e:
                    print(f"❌ Error processing message: {e}")
                    # Try to decode raw bytes for debugging
                    try:
                        print(f"Raw key: {msg.key()}")
                        print(f"Raw value length: {len(msg.value())} bytes")
                        # Show first few bytes for debugging
                        if len(msg.value()) > 20:
                            print(f"First 20 bytes: {msg.value()[:20]}")
                    except:
                        pass
                    
        except KeyboardInterrupt:
            print("🛑 Interrupted by user")
        finally:
            print(f"\n📊 Consumed {self.message_count} messages total")
            print(f"📈 Successfully enriched: {self.enriched_count} messages")
            print(f"📉 Fallback enrichment: {self.message_count - self.enriched_count} messages")
            print("🔒 Closing consumer...")
            self.consumer.close()
            print(f"✅ Consumer completed: {self.message_count} total messages, {self.enriched_count} enriched")

def main():
    consumer = ClickStreamConsumer()
    
    if consumer.setup():
        consumer.consume_messages()

if __name__ == "__main__":
    main()
