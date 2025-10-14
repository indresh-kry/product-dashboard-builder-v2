#!/usr/bin/env python3
"""
System Health Check Script for Product Dashboard Builder v2
"""
import os
import sys
import json
from google.cloud import bigquery
from google.oauth2 import service_account

def check_environment():
    """Check if all required environment variables are set"""
    required_vars = ['RUN_HASH', 'DATASET_NAME', 'GOOGLE_CLOUD_PROJECT']
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {', '.join(missing_vars)}")
        return False
    
    print("✅ All required environment variables are set")
    return True

def check_bigquery_connection():
    """Check BigQuery connection"""
    try:
        credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        if not credentials_path or not os.path.exists(credentials_path):
            print("❌ Google Cloud credentials file not found")
            return False
        
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials, project=os.environ.get('GOOGLE_CLOUD_PROJECT'))
        
        # Test connection with a simple query
        query = "SELECT 1 as test"
        result = client.query(query).result()
        print("✅ BigQuery connection successful")
        return True
        
    except Exception as e:
        print(f"❌ BigQuery connection failed: {str(e)}")
        return False

def main():
    """Main health check function"""
    print("🔍 Running system health checks...")
    
    # Check environment
    env_ok = check_environment()
    
    # Check BigQuery connection
    bq_ok = check_bigquery_connection()
    
    if env_ok and bq_ok:
        print("✅ All system health checks passed")
        return 0
    else:
        print("❌ Some system health checks failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
