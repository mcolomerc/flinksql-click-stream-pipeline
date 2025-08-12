#!/usr/bin/env python3
"""
Pipeline Cleanup Script
Cleans up all pipeline resources including topics and Flink statements
"""

import sys
from pathlib import Path

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from config import config
from topics import delete_topics
from flink_sql import FlinkSQLManager

def cleanup_pipeline():
    """Cleanup all pipeline resources"""
    print("🧹 Cleaning up pipeline resources...")
    print(f"📋 Pipeline ID: {config.pipeline_id}")
    
    # Cleanup Flink statements
    flink_manager = FlinkSQLManager()
    if flink_manager.setup():
        flink_manager.cleanup_statements()
    
    # Delete topics
    delete_topics()
    
    print("✅ Pipeline cleanup completed")

if __name__ == "__main__":
    cleanup_pipeline()
