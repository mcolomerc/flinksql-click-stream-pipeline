#!/usr/bin/env python3
"""
Flink SQL Statements Management
Creates and executes FlinkSQL statements for the pipeline
"""

import os
import requests
import time
import base64
from pathlib import Path
from config import config

class FlinkSQLManager:
    def __init__(self):
        self.base_url = None
        self.headers = None
        self.statements = []
        
    def setup(self):
        """Initialize Flink SQL client"""
        if not config.validate_config():
            return False
        
        # Get required configuration - use CONFLUENT_ENV_ID like in final folder
        org_id = config.config.get('FLINK_ORG_ID')
        env_id = config.config.get('CONFLUENT_ENV_ID') or config.config.get('CONFLUENT_CLOUD_ENVIRONMENT_ID', 'env-default')
        
        if not org_id:
            print("❌ FLINK_ORG_ID not found in configuration")
            return False
            
        if not env_id or env_id == 'env-default':
            print("❌ CONFLUENT_ENV_ID not found in configuration")
            return False
            
        self.base_url = f"{config.config['FLINK_REST_ENDPOINT']}/sql/v1/organizations/{org_id}/environments/{env_id}"
        self.headers = {
            'Authorization': config.get_flink_auth_header(),
            'Content-Type': 'application/json'
        }
        
        print(f"✅ Flink SQL Manager initialized: {config.config['FLINK_REST_ENDPOINT']}")
        return True
    
    def load_sql_files(self):
        """Load and prepare SQL files with variable substitution"""
        sql_dir = Path(__file__).parent / "sql"
        
        # Get configuration values
        topic_names = config.get_topic_names()
        
        # Template variables
        template_vars = {
            'enriched_table': f"enriched_view_{config.pipeline_id}",
            'input_topic': topic_names['input'],
            'output_topic': topic_names['output'],
            'pipeline_id': config.pipeline_id,
            'bootstrap_servers': config.config['BOOTSTRAP_SERVERS'],
            'sasl_username': config.config['SASL_USERNAME'],
            'sasl_password': config.config['SASL_PASSWORD']
        }
        
        # Automatically discover and sort SQL files by name
        sql_files = sorted([f.name for f in sql_dir.glob("*.sql")])
        print(f"📁 Found SQL files: {sql_files}")
        
        self.statements = []
        
        for sql_file in sql_files:
            file_path = sql_dir / sql_file
            if file_path.exists():
                with open(file_path, 'r') as f:
                    sql_content = f.read()
                
                # Substitute template variables
                for key, value in template_vars.items():
                    sql_content = sql_content.replace(f'{{{key}}}', value)
                
                # Split multiple statements by semicolon and filter out empty/comment-only statements
                statements = []
                
                # If the content doesn't contain semicolons, treat the whole content as one statement
                if ';' not in sql_content:
                    cleaned_content = sql_content.strip()
                    # Simple check: if it contains INSERT, CREATE, etc., it's a valid statement
                    if (cleaned_content and 
                        any(keyword in cleaned_content.upper() for keyword in ['INSERT', 'CREATE', 'SELECT', 'UPDATE', 'DELETE', 'ALTER']) and
                        len(cleaned_content) > 50):
                        statements.append(cleaned_content)
                else:
                    # Split by semicolon for multiple statements
                    for stmt in sql_content.split(';'):
                        stmt = stmt.strip()
                        if (stmt and 
                            any(keyword in stmt.upper() for keyword in ['INSERT', 'CREATE', 'SELECT', 'UPDATE', 'DELETE', 'ALTER']) and
                            len(stmt) > 50):
                            statements.append(stmt)
                
                # Add each statement separately
                for i, stmt in enumerate(statements):
                    stmt_name = f"{sql_file}_part_{i+1}" if len(statements) > 1 else sql_file
                    self.statements.append({
                        'name': stmt_name,
                        'sql': stmt
                    })
                
                print(f"📄 Loaded SQL file: {sql_file} ({len(statements)} statements)")
            else:
                print(f"❌ SQL file not found: {sql_file}")
                return False
        
        print(f"✅ Loaded {len(self.statements)} SQL statements")
        return True
    
    def execute_statement(self, statement_name, statement_sql):
        """Execute a single SQL statement using the final folder approach"""
        print(f"🚀 Executing: {statement_name}")
        
        # Create payload like in final folder
        # Ensure name follows Confluent naming rules: lowercase alphanumeric and hyphens only
        safe_name = statement_name.lower().replace('_', '-').replace(' ', '-').replace('.sql', '')
        unique_name = f"{safe_name}-{int(time.time())}"
        
        # Ensure name is under 100 characters
        if len(unique_name) > 100:
            # Truncate the safe_name part but keep the timestamp
            max_safe_length = 100 - len(str(int(time.time()))) - 1  # -1 for the dash
            safe_name = safe_name[:max_safe_length]
            unique_name = f"{safe_name}-{int(time.time())}"
        
        payload = {
            "name": unique_name,
            "spec": {
                "statement": statement_sql,
                "compute_pool_id": config.config.get('FLINK_COMPUTE_POOL_ID'),
                "properties": {
                    "sql.current-catalog": config.config.get('SQL_CURRENT_CATALOG'),
                    "sql.current-database": config.config.get('SQL_CURRENT_DATABASE')
                }
            }
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/statements",
                headers=self.headers,
                json=payload,
                timeout=60
            )
            
            if response.status_code in [200, 201]:
                result = response.json()
                statement_id = result.get('name', 'unknown')
                print(f"   ✅ Statement executed successfully")
                print(f"   📋 Statement ID: {statement_id}")
                
                # Check statement status to ensure it's running properly
                if not self.check_statement_status(statement_id):
                    return False
                
                # For INSERT statements, return the ID to track execution
                if statement_sql.strip().upper().startswith('INSERT'):
                    return statement_id
                    
                return True
            elif response.status_code == 409:
                print(f"   ⚠️  Statement already exists")
                return True
            else:
                print(f"   ❌ Failed to execute statement: {response.status_code}")
                print(f"   📋 Error Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"   ❌ Error executing statement: {e}")
            return False
    
    def check_statement_status(self, statement_id):
        """Check if a statement is running successfully"""
        try:
            response = requests.get(
                f"{self.base_url}/statements/{statement_id}",
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                status = result.get('status', {}).get('phase', 'UNKNOWN')
                
                if status == 'RUNNING':
                    print(f"   ✅ Statement {statement_id} is running")
                    return True
                elif status == 'COMPLETED':
                    print(f"   ✅ Statement {statement_id} completed successfully")
                    return True
                elif status == 'FAILED':
                    error_msg = result.get('status', {}).get('detail', 'Unknown error')
                    print(f"   ❌ Statement {statement_id} failed: {error_msg}")
                    return False
                elif status in ['PENDING', 'PROVISIONING']:
                    print(f"   ⏳ Statement {statement_id} is {status.lower()}, waiting...")
                    time.sleep(5)
                    return self.check_statement_status(statement_id)  # Retry
                else:
                    print(f"   ⚠️  Statement {statement_id} status: {status}")
                    return True  # Continue for other statuses
            else:
                print(f"   ❌ Failed to check statement status: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ❌ Error checking statement status: {e}")
            return False
    
    def deploy_pipeline(self):
        """Deploy the complete FlinkSQL pipeline"""
        print("🚀 Deploying FlinkSQL pipeline...")
        
        if not self.load_sql_files():
            return False
        
        insert_handles = []
        
        for statement in self.statements:
            result = self.execute_statement(statement['name'], statement['sql'])
            
            if result is False:
                print(f"❌ Failed to execute {statement['name']}")
                return False
            
            # If we got a statement ID, check its status
            if isinstance(result, str):
                print(f"   📝 Statement submitted: {result}")
                
                # Wait for statement to start
                if statement['sql'].strip().upper().startswith('ALTER'):
                    time.sleep(5)  # ALTER statements need less time
                elif statement['sql'].strip().upper().startswith('CREATE'):
                    time.sleep(10)  # CREATE statements need more time
                else:
                    time.sleep(8)   # INSERT statements
                
                # Check status
                if not self.check_statement_status(result):
                    print(f"❌ Statement {statement['name']} failed - stopping pipeline")
                    return False
                
                # Track INSERT statement handles
                if statement['name'].startswith('01_direct_insert') or 'INSERT' in statement['sql'].upper():
                    insert_handles.append(result)
            
        print("✅ Pipeline deployed successfully!")
        
        if insert_handles:
            print(f"📋 Active INSERT statements: {insert_handles}")
            
        return True
    
    def cleanup_statements(self):
        """Stop and cleanup Flink statements"""
        print("🧹 Cleaning up Flink statements...")
        
        try:
            # Get all statements
            response = requests.get(
                f"{self.base_url}/v1/statements",
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                statements = response.json().get('data', [])
                
                for stmt in statements:
                    statement_handle = stmt.get('statement_handle')
                    status = stmt.get('status', {}).get('status')
                    
                    if status in ['RUNNING', 'PENDING']:
                        print(f"🛑 Stopping statement: {statement_handle}")
                        
                        # Stop the statement
                        stop_response = requests.delete(
                            f"{self.base_url}/v1/statements/{statement_handle}",
                            headers=self.headers,
                            timeout=30
                        )
                        
                        if stop_response.status_code == 200:
                            print(f"   ✅ Stopped: {statement_handle}")
                        else:
                            print(f"   ❌ Failed to stop: {statement_handle}")
                            
        except Exception as e:
            print(f"❌ Error during cleanup: {e}")

def main():
    """Main function to deploy the pipeline"""
    manager = FlinkSQLManager()
    
    if not manager.setup():
        print("❌ Failed to setup Flink SQL manager")
        return False
    
    try:
        return manager.deploy_pipeline()
    except Exception as e:
        print(f"❌ Pipeline deployment failed: {e}")
        return False

if __name__ == "__main__":
    main()
