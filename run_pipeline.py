#!/usr/bin/env python3
"""
Main Pipeline Runner
Orchestrates the complete click stream enrichment pipeline
"""

import sys
import time
import threading
from pathlib import Path

# Add current directory to path for imports
sys.path.append(str(Path(__file__).parent))

from config import config
from topics import create_topics, delete_topics
from flink_sql import FlinkSQLManager
from producer import ClickStreamProducer
from consumer import OutputConsumer

class PipelineRunner:
    def __init__(self):
        self.flink_manager = None
        self.producer = None
        self.consumer = None
        
    def setup(self):
        """Setup all pipeline components"""
        print("🔧 Setting up pipeline components...")
        
        if not config.validate_config():
            return False
        
        # Initialize components
        self.flink_manager = FlinkSQLManager()
        self.producer = ClickStreamProducer()
        self.consumer = OutputConsumer()
        
        # Setup components
        if not self.flink_manager.setup():
            return False
        if not self.producer.setup():
            return False
        if not self.consumer.setup():
            return False
            
        print("✅ All components setup successfully")
        return True
    
    def run_pipeline(self):
        """Run the complete pipeline"""
        print("🚀 Starting Click Stream Enrichment Pipeline")
        print(f"📋 Pipeline ID: {config.pipeline_id}")
        
        try:
            # Step 1: Create Kafka topics
            print("\n" + "="*50)
            print("STEP 1: Creating Kafka Topics")
            print("="*50)
            if not create_topics():
                return False
            
            # Step 2: Deploy Flink SQL pipeline
            print("\n" + "="*50)
            print("STEP 2: Deploying Flink SQL Pipeline")
            print("="*50)
            if not self.flink_manager.deploy_pipeline():
                return False
            
            # Wait for Flink pipeline to be ready
            print("\n⏳ Waiting for Flink pipeline to initialize...")
            time.sleep(10)
            
            # Step 3: Generate events first
            print("\n" + "="*50)
            print("STEP 3: Generating Click Stream Events")
            print("="*50)
            if not self.producer.generate_events():
                return False
            
            # Wait for events to be processed by Flink
            print("\n⏳ Waiting for Flink to process events...")
            time.sleep(20)
            
            # Step 4: Consume enriched results
            print("\n" + "="*50)
            print("STEP 4: Consuming Enriched Results")
            print("="*50)
            
            # We expect 5 enriched messages (5 product clicks out of 10 total events)
            message_count, enriched_count = self.consumer.consume_messages(
                timeout_seconds=120, 
                expected_messages=5
            )
            
            print(f"\n📊 Results: {message_count} total messages, {enriched_count} enriched events")
            
            if enriched_count > 0:
                print("\n" + "="*50)
                print("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
                print("="*50)
                print("✅ Pipeline executed successfully!")
                print(f"   • Generated 10 click events")
                print(f"   • Processed {enriched_count} enriched product clicks")
                return True
            else:
                print("\n❌ No enriched events were processed")
                return False
            
        except Exception as e:
            print(f"\n❌ Pipeline execution failed: {e}")
            return False
        
        finally:
            # Always cleanup, but only do full cleanup on successful completion
            self.cleanup(full_cleanup=True)
    
    def cleanup(self, full_cleanup=True):
        """Cleanup pipeline resources"""
        print("\n🧹 Cleaning up pipeline resources...")
        
        try:
            # Stop Flink statements
            if self.flink_manager:
                print("   • Stopping Flink statements...")
                self.flink_manager.cleanup_statements()
            
            # Close producer and consumer
            if self.producer:
                print("   • Closing producer...")
                self.producer.close()
            if self.consumer:
                print("   • Closing consumer...")
                self.consumer.close()
            
            # Full cleanup if requested
            if full_cleanup:
                print("   • Deleting Kafka topics...")
                delete_topics()
                
        except Exception as e:
            print(f"⚠️  Error during cleanup: {e}")
        
        print("✅ Cleanup completed")
    
def main():
    """Main function"""
    print("🎯 Click Stream Enrichment Pipeline")
    print("=" * 50)
    
    runner = PipelineRunner()
    
    if not runner.setup():
        print("❌ Failed to setup pipeline")
        return False
    
    try:
        success = runner.run_pipeline()
        
        if success:
            print("\n🎉 Pipeline completed successfully!")
            print("\n📋 Summary:")
            print("   • Created input/output topics")
            print("   • Deployed Flink SQL enrichment pipeline")
            print("   • Generated 10 click events (5 per user)")
            print("   • Enriched product clicks with search IDs")
            print("   • Consumed and displayed enriched results")
            print("   • Cleaned up all resources")
        else:
            print("\n❌ Pipeline failed")
            
        return success
        
    except KeyboardInterrupt:
        print("\n🛑 Pipeline interrupted by user")
        runner.cleanup(full_cleanup=False)  # Don't delete topics on interruption
        return False
    
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        runner.cleanup(full_cleanup=False)  # Don't delete topics on error
        return False

if __name__ == "__main__":
    main()
