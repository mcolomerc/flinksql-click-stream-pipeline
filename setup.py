#!/usr/bin/env python3
"""
Pipeline Setup Script
Helps with initial configuration and validation
"""

import os
import sys
from pathlib import Path

def setup_environment():
    """Setup environment file"""
    pipeline_dir = Path(__file__).parent
    env_file = pipeline_dir / ".env"
    example_file = pipeline_dir / ".env.example"
    template_file = pipeline_dir / ".env.template"
    
    print("🔧 Pipeline Setup")
    print("=" * 30)
    
    # Check if .env already exists
    if env_file.exists():
        print("✅ .env file already exists")
        
        # Validate configuration
        try:
            from config import config
            if config.validate_config():
                print("✅ Configuration is valid")
                print(f"📋 Pipeline ID: {config.pipeline_id}")
                return True
            else:
                print("❌ Configuration validation failed")
                return False
        except Exception as e:
            print(f"❌ Error validating configuration: {e}")
            return False
    
    # Create .env from example if available
    if example_file.exists():
        print("📄 Copying .env.example to .env...")
        
        with open(example_file, 'r') as src:
            content = src.read()
        
        with open(env_file, 'w') as dst:
            dst.write(content)
            
        print("✅ Created .env file from example")
        
    elif template_file.exists():
        print("📄 Copying .env.template to .env...")
        
        with open(template_file, 'r') as src:
            content = src.read()
        
        with open(env_file, 'w') as dst:
            dst.write(content)
            
        print("✅ Created .env file from template")
    
    else:
        print("❌ No template file found")
        return False
    
    print("\n📋 Configuration Required:")
    print("Please edit .env file and update the following:")
    print("  • CONFLUENT_CLOUD_ENVIRONMENT_ID")
    print("  • CONFLUENT_CLOUD_CLUSTER_ID") 
    print("  • FLINK_REST_ENDPOINT")
    print("  • FLINK_ORG_ID")
    print("  • FLINK_API_KEY")
    print("  • FLINK_API_SECRET")
    print("  • SCHEMA_REGISTRY_API_KEY")
    print("  • SCHEMA_REGISTRY_API_SECRET")
    print("  • SCHEMA_REGISTRY_ENDPOINT")
    print("\nThen run: python run_pipeline.py")
    
    return False

def install_dependencies():
    """Install required Python packages"""
    print("\n📦 Installing dependencies...")
    
    requirements_file = Path(__file__).parent / "requirements.txt"
    
    if not requirements_file.exists():
        print("❌ requirements.txt not found")
        return False
    
    try:
        import subprocess
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", "-r", str(requirements_file)
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Dependencies installed successfully")
            return True
        else:
            print(f"❌ Failed to install dependencies: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error installing dependencies: {e}")
        return False

def main():
    """Main setup function"""
    print("🎯 Click Stream Pipeline Setup")
    print("=" * 40)
    
    # Install dependencies
    if not install_dependencies():
        print("❌ Setup failed during dependency installation")
        return False
    
    # Setup environment
    if setup_environment():
        print("\n🎉 Setup completed! Ready to run pipeline.")
        print("Run: python run_pipeline.py")
        return True
    else:
        print("\n⚠️  Setup completed with configuration required.")
        print("Please update .env file and run again.")
        return False

if __name__ == "__main__":
    main()
