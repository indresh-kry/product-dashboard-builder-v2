#!/usr/bin/env python3
"""
Enhanced Data Aggregation Script for Product Dashboard Builder v2
Attempts to create BigQuery table, falls back to CSV if no permissions
"""
import os
import json
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_bigquery_client():
    """Initialize BigQuery client with credentials"""
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=project_id)
    return client

def load_schema_mapping(run_hash):
    """Load schema mapping from Phase 1 results"""
    schema_file = f'run_logs/{run_hash}/outputs/schema/schema_mapping.json'
    
    if not os.path.exists(schema_file):
        raise FileNotFoundError(f"Schema mapping file not found: {schema_file}")
    
    with open(schema_file, 'r') as f:
        schema_mapping = json.load(f)
    
    return schema_mapping

def generate_aggregation_sql(dataset_name, schema_mapping, limit=None):
    """Generate SQL for creating the aggregation table"""
    print("📝 Generating aggregation SQL...")
    
    # Get filter information from environment variables
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    
    # Build WHERE clause
    conditions = []
    if app_filter and app_filter != 'ALL_APPS':
        conditions.append(f"app_longname = '{app_filter}'")
    if date_start and date_end and date_start != 'ALL_DATES' and date_end != 'ALL_DATES':
        conditions.append(f"DATE(adjusted_timestamp) BETWEEN '{date_start}' AND '{date_end}'")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    # Get table creation settings from environment
    target_project = os.environ.get('TARGET_PROJECT', os.environ.get('GOOGLE_CLOUD_PROJECT'))
    target_dataset = os.environ.get('TARGET_DATASET', 'nbs_dataset')
    table_name = os.environ.get('AGGREGATION_TABLE_NAME', 'user_daily_aggregation')
    
    # Build the complete SQL for table creation
    sql = f"""
-- Enhanced Data Aggregation Table Generation
-- Generated for run: {schema_mapping.get('run_hash', 'unknown')}
-- Dataset: {dataset_name}
-- Filters: {schema_mapping.get('filters', {})}

CREATE OR REPLACE TABLE `{target_project}.{target_dataset}.{table_name}` AS
SELECT 
    -- Primary Key
    COALESCE(custom_user_id, device_id) as user_id,
    device_id,
    DATE(adjusted_timestamp) as date,
    
    -- Session Metrics
    COUNT(DISTINCT CASE WHEN name = '__SESSION__' THEN session_id END) as total_sessions,
    
    -- Revenue Metrics
    SUM(CASE WHEN is_revenue_valid = true THEN converted_revenue ELSE 0 END) as total_revenue,
    SUM(CASE WHEN is_revenue_valid = true AND converted_currency = 'USD' THEN converted_revenue ELSE 0 END) as total_revenue_usd,
    SUM(CASE WHEN is_revenue_valid = true AND received_revenue_event LIKE '%iap%' THEN converted_revenue ELSE 0 END) as iap_revenue,
    SUM(CASE WHEN is_revenue_valid = true AND received_revenue_event LIKE '%ad%' THEN converted_revenue ELSE 0 END) as ad_revenue,
    COUNT(CASE WHEN is_revenue_valid = true AND received_revenue_event LIKE '%iap%' THEN 1 END) as iap_events_count,
    COUNT(CASE WHEN is_revenue_valid = true AND received_revenue_event LIKE '%ad%' THEN 1 END) as ad_events_count,
    COUNT(CASE WHEN is_revenue_valid = true THEN 1 END) as total_revenue_events_count,
    MIN(CASE WHEN is_revenue_valid = true THEN adjusted_timestamp END) as first_purchase_time,
    
    -- Event Counts & Engagement Metrics
    COUNT(*) as total_events,
    COUNT(DISTINCT name) as unique_events,
    
    -- Key Level Events
    MIN(CASE WHEN name = 'ftue_complete' THEN adjusted_timestamp END) as ftue_complete_time,
    MIN(CASE WHEN name = 'div_level_1' THEN adjusted_timestamp END) as level_1_time,
    MIN(CASE WHEN name = 'div_level_2' THEN adjusted_timestamp END) as level_2_time,
    MIN(CASE WHEN name = 'div_level_3' THEN adjusted_timestamp END) as level_3_time,
    MIN(CASE WHEN name = 'div_level_4' THEN adjusted_timestamp END) as level_4_time,
    MIN(CASE WHEN name = 'div_level_5' THEN adjusted_timestamp END) as level_5_time,
    MIN(CASE WHEN name = 'div_level_6' THEN adjusted_timestamp END) as level_6_time,
    MIN(CASE WHEN name = 'div_level_7' THEN adjusted_timestamp END) as level_7_time,
    
    -- Revenue Events
    MIN(CASE WHEN name = '__ADMON_USER_LEVEL_REVENUE__' THEN adjusted_timestamp END) as admon_revenue_time,
    
    -- Geographic & Attribution
    ANY_VALUE(country) as country,
    ANY_VALUE(state) as state,
    ANY_VALUE(city) as city,
    ANY_VALUE(install_source) as acquisition_channel,
    ANY_VALUE(campaign_id) as campaign_id,
    ANY_VALUE(campaign_name) as campaign_name,
    ANY_VALUE(utm_source) as utm_source,
    ANY_VALUE(utm_campaign) as utm_campaign,
    
    -- Data Quality & Metadata
    {schema_mapping.get('data_quality_score', 0)} as data_quality_score,
    CURRENT_TIMESTAMP() as last_updated,
    '{schema_mapping.get('run_hash', 'unknown')}' as run_hash,
    ANY_VALUE(app_longname) as app_name

FROM `{dataset_name}`
{where_clause}
GROUP BY 
    COALESCE(custom_user_id, device_id),
    device_id,
    DATE(adjusted_timestamp)
ORDER BY 
    COALESCE(custom_user_id, device_id),
    DATE(adjusted_timestamp);
"""
    
    return sql

def generate_sample_query(dataset_name, schema_mapping, limit=1000):
    """Generate a query for sampling data (for CSV fallback)"""
    print("📊 Generating sample data query...")
    
    # Get filter information from environment variables
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    
    # Build WHERE clause
    conditions = []
    if app_filter and app_filter != 'ALL_APPS':
        conditions.append(f"app_longname = '{app_filter}'")
    if date_start and date_end and date_start != 'ALL_DATES' and date_end != 'ALL_DATES':
        conditions.append(f"DATE(adjusted_timestamp) BETWEEN '{date_start}' AND '{date_end}'")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    # Build the sample query
    query = f"""
-- Sample Data Query (First {limit} rows of what the aggregation table would look like)
SELECT 
    -- Primary Key
    COALESCE(custom_user_id, device_id) as user_id,
    device_id,
    DATE(adjusted_timestamp) as date,
    
    -- Session Metrics
    COUNT(DISTINCT CASE WHEN name = '__SESSION__' THEN session_id END) as total_sessions,
    
    -- Revenue Metrics
    SUM(CASE WHEN is_revenue_valid = true THEN converted_revenue ELSE 0 END) as total_revenue,
    SUM(CASE WHEN is_revenue_valid = true AND converted_currency = 'USD' THEN converted_revenue ELSE 0 END) as total_revenue_usd,
    SUM(CASE WHEN is_revenue_valid = true AND received_revenue_event LIKE '%iap%' THEN converted_revenue ELSE 0 END) as iap_revenue,
    SUM(CASE WHEN is_revenue_valid = true AND received_revenue_event LIKE '%ad%' THEN converted_revenue ELSE 0 END) as ad_revenue,
    COUNT(CASE WHEN is_revenue_valid = true AND received_revenue_event LIKE '%iap%' THEN 1 END) as iap_events_count,
    COUNT(CASE WHEN is_revenue_valid = true AND received_revenue_event LIKE '%ad%' THEN 1 END) as ad_events_count,
    COUNT(CASE WHEN is_revenue_valid = true THEN 1 END) as total_revenue_events_count,
    MIN(CASE WHEN is_revenue_valid = true THEN adjusted_timestamp END) as first_purchase_time,
    
    -- Event Counts & Engagement Metrics
    COUNT(*) as total_events,
    COUNT(DISTINCT name) as unique_events,
    
    -- Key Level Events
    MIN(CASE WHEN name = 'ftue_complete' THEN adjusted_timestamp END) as ftue_complete_time,
    MIN(CASE WHEN name = 'div_level_1' THEN adjusted_timestamp END) as level_1_time,
    MIN(CASE WHEN name = 'div_level_2' THEN adjusted_timestamp END) as level_2_time,
    MIN(CASE WHEN name = 'div_level_3' THEN adjusted_timestamp END) as level_3_time,
    MIN(CASE WHEN name = 'div_level_4' THEN adjusted_timestamp END) as level_4_time,
    MIN(CASE WHEN name = 'div_level_5' THEN adjusted_timestamp END) as level_5_time,
    MIN(CASE WHEN name = 'div_level_6' THEN adjusted_timestamp END) as level_6_time,
    MIN(CASE WHEN name = 'div_level_7' THEN adjusted_timestamp END) as level_7_time,
    
    -- Revenue Events
    MIN(CASE WHEN name = '__ADMON_USER_LEVEL_REVENUE__' THEN adjusted_timestamp END) as admon_revenue_time,
    
    -- Geographic & Attribution
    ANY_VALUE(country) as country,
    ANY_VALUE(state) as state,
    ANY_VALUE(city) as city,
    ANY_VALUE(install_source) as acquisition_channel,
    ANY_VALUE(campaign_id) as campaign_id,
    ANY_VALUE(campaign_name) as campaign_name,
    ANY_VALUE(utm_source) as utm_source,
    ANY_VALUE(utm_campaign) as utm_campaign,
    
    -- Data Quality & Metadata
    {schema_mapping.get('data_quality_score', 0)} as data_quality_score,
    CURRENT_TIMESTAMP() as last_updated,
    '{schema_mapping.get('run_hash', 'unknown')}' as run_hash,
    ANY_VALUE(app_longname) as app_name

FROM `{dataset_name}`
{where_clause}
GROUP BY 
    COALESCE(custom_user_id, device_id),
    device_id,
    DATE(adjusted_timestamp)
ORDER BY 
    COALESCE(custom_user_id, device_id),
    DATE(adjusted_timestamp)
LIMIT {limit};
"""
    
    return query

def attempt_table_creation(client, aggregation_sql, run_hash):
    """Attempt to create the aggregation table in BigQuery"""
    print("🔨 Attempting to create aggregation table in BigQuery...")
    
    try:
        job = client.query(aggregation_sql)
        job.result()  # Wait for completion
        print("✅ Aggregation table created successfully in BigQuery!")
        return True
    except Exception as e:
        print(f"❌ Failed to create table in BigQuery: {str(e)}")
        print("📋 Falling back to CSV export...")
        return False

def create_csv_fallback(client, sample_query, run_hash, limit):
    """Create CSV file as fallback when table creation fails"""
    print("📊 Creating CSV file as fallback...")
    
    try:
        sample_df = client.query(sample_query).to_dataframe()
        print(f"✅ Retrieved {len(sample_df)} sample rows")
        
        # Save sample data
        sample_csv = f'run_logs/{run_hash}/outputs/aggregations/aggregation_data_{limit}_rows.csv'
        os.makedirs(os.path.dirname(sample_csv), exist_ok=True)
        sample_df.to_csv(sample_csv, index=False)
        
        print(f"✅ CSV data saved to: {sample_csv}")
        return sample_csv, sample_df
    except Exception as e:
        print(f"❌ Error creating CSV: {str(e)}")
        raise

def main():
    """Main aggregation function"""
    print("🚀 Starting Enhanced Data Aggregation for Product Dashboard Builder v2")
    print("=" * 80)
    
    # Get environment variables
    run_hash = os.environ.get('RUN_HASH')
    dataset_name = os.environ.get('DATASET_NAME')
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    limit = int(os.environ.get('AGGREGATION_LIMIT', '1000'))
    
    print(f"Run Hash: {run_hash}")
    print(f"Dataset: {dataset_name}")
    print(f"App Filter: {app_filter if app_filter else 'ALL_APPS'}")
    print(f"Date Range: {date_start if date_start else 'ALL_DATES'} to {date_end if date_end else 'ALL_DATES'}")
    print(f"Limit: {limit}")
    print()
    
    try:
        # Initialize BigQuery client
        client = get_bigquery_client()
        
        # Load schema mapping from Phase 1
        schema_mapping = load_schema_mapping(run_hash)
        print("✅ Loaded schema mapping from Phase 1")
        
        # Generate aggregation SQL for table creation
        aggregation_sql = generate_aggregation_sql(dataset_name, schema_mapping)
        
        # Save SQL to file
        sql_file = f'run_logs/{run_hash}/working/aggregation_sql_enhanced.sql'
        with open(sql_file, 'w') as f:
            f.write(aggregation_sql)
        
        print(f"✅ Aggregation SQL saved to: {sql_file}")
        
        # Attempt to create table in BigQuery
        table_created = attempt_table_creation(client, aggregation_sql, run_hash)
        
        if not table_created:
            # Fallback to CSV creation
            sample_query = generate_sample_query(dataset_name, schema_mapping, limit)
            
            sample_file = f'run_logs/{run_hash}/working/sample_data_query_enhanced.sql'
            with open(sample_file, 'w') as f:
                f.write(sample_query)
            
            print(f"✅ Sample data query saved to: {sample_file}")
            
            # Create CSV fallback
            csv_file, sample_df = create_csv_fallback(client, sample_query, run_hash, limit)
            
            # Display sample data
            print(f"\n📋 Sample Data (First {limit} rows of what the aggregation table would look like):")
            print("=" * 80)
            print(sample_df.to_string(index=False))
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during aggregation: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
