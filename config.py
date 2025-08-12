#!/usr/bin/env python3
"""
Pipeline Configuration and Environment Setup
"""

import os
import uuid
import time
from pathlib import Path
from typing import Dict, Any

class PipelineConfig:
    def __init__(self):
        self.pipeline_id = None
        self.config = {}
        self.setup_environment()
        
    def setup_environment(self):
        """Load environment variables from .env file"""
        env_file = Path(__file__).parent / ".env"
        
        if not env_file.exists():
            print("❌ .env file not found. Please copy .env.template to .env and configure it.")
            raise FileNotFoundError("Configuration file .env not found")
            
        # Load environment variables
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip().strip('"\'')
                    if value:  # Only set non-empty values
                        os.environ[key] = value
                        self.config[key] = value
        
        # Generate pipeline ID if not set
        if not self.config.get('PIPELINE_ID'):
            self.pipeline_id = f"pipeline_{int(time.time())}"
            os.environ['PIPELINE_ID'] = self.pipeline_id
            self.config['PIPELINE_ID'] = self.pipeline_id
        else:
            self.pipeline_id = self.config['PIPELINE_ID']
            
        print(f"🔧 Pipeline ID: {self.pipeline_id}")
        
    def get_kafka_config(self) -> Dict[str, str]:
        """Get Kafka client configuration"""
        return {
            'bootstrap.servers': self.config['BOOTSTRAP_SERVERS'],
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.config['SASL_USERNAME'],
            'sasl.password': self.config['SASL_PASSWORD']
        }
    
    def get_topic_names(self) -> Dict[str, str]:
        """Get topic names with pipeline ID"""
        return {
            'input': f"input_{self.pipeline_id}",
            'output': f"output_{self.pipeline_id}"
        }
    
    def get_table_names(self) -> Dict[str, str]:
        """Get table names with pipeline ID"""
        return {
            'source': f"source_table_{self.pipeline_id}",
            'enriched': f"enriched_table_{self.pipeline_id}",
            'sink': f"sink_table_{self.pipeline_id}"
        }
    
    def get_schema_registry_config(self) -> Dict[str, str]:
        """Get Schema Registry client configuration"""
        return {
            'url': self.config['SCHEMA_REGISTRY_ENDPOINT'],
            'basic.auth.user.info': f"{self.config['SCHEMA_REGISTRY_API_KEY']}:{self.config['SCHEMA_REGISTRY_API_SECRET']}"
        }
    
    def get_flink_auth_header(self) -> str:
        """Get Flink API authentication header"""
        import base64
        credentials = f"{self.config['FLINK_API_KEY']}:{self.config['FLINK_API_SECRET']}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        return f"Basic {encoded_credentials}"
    
    def validate_config(self) -> bool:
        """Validate all required configuration is present"""
        required_vars = [
            'BOOTSTRAP_SERVERS', 'SASL_USERNAME', 'SASL_PASSWORD',
            'FLINK_REST_ENDPOINT', 'FLINK_API_KEY', 'FLINK_API_SECRET',
            'CONFLUENT_CLOUD_ENVIRONMENT_ID', 'SCHEMA_REGISTRY_API_KEY',
            'SCHEMA_REGISTRY_API_SECRET', 'SCHEMA_REGISTRY_ENDPOINT'
        ]
        
        missing = [var for var in required_vars if not self.config.get(var)]
        
        if missing:
            print(f"❌ Missing required configuration: {', '.join(missing)}")
            return False
            
        print("✅ Configuration validated successfully")
        return True

# Global instance
config = PipelineConfig()
