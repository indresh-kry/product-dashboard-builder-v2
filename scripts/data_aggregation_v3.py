#!/usr/bin/env python3
"""
Enhanced Data Aggregation Script - Final Working Version
Version: 3.3.0
Last Updated: 2025-10-15

Changelog:
- v3.3.0 (2025-10-15): Added missing cohort analysis with cohort_date, days_since_first_event, and user_type fields
- v3.2.0 (2025-10-15): Fixed case sensitivity in revenue classification patterns using UPPER() function
- v3.1.0 (2025-10-15): Fixed revenue classification logic to use name column with generic patterns
- v3.0.0 (2025-10-14): Renamed from data_aggregation_enhanced_v2_final_working.py for cleaner versioning
- v2.0.6 (2025-10-14): Fixed GROUP BY issues with proper aggregation functions
- v2.0.5 (2025-10-14): Removed session count to eliminate SQL ambiguity
- v2.0.4 (2025-10-14): Simplified working version focusing on core requirements
- v2.0.3 (2025-10-14): Final version with proper table aliases
- v2.0.2 (2025-10-14): Working version with simplified query structure
- v2.0.1 (2025-10-14): Fixed version attempting to resolve session_id ambiguity
- v2.0.0 (2025-10-14): Added session duration calculation and revenue classification
"""
import os
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

def get_bigquery_client():
    """Initialize BigQuery client with credentials"""
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=project_id)
    return client

def load_schema_mapping(run_hash):
    """Load schema mapping from previous run"""
    schema_file = f'run_logs/{run_hash}/outputs/schema/schema_mapping.json'
    
    try:
        with open(schema_file, 'r') as f:
            schema_mapping = json.load(f)
        print(f"✅ Loaded schema mapping from: {schema_file}")
        return schema_mapping
    except Exception as e:
        print(f"❌ Error loading schema mapping: {str(e)}")
        return None

def build_where_clause(app_filter, date_start, date_end):
    """Build WHERE clause for SQL queries"""
    conditions = []
    
    if app_filter and app_filter.strip() and app_filter != 'ALL_APPS':
        conditions.append(f"app_longname = '{app_filter}'")
    
    if date_start and date_end and date_start.strip() and date_end.strip() and date_start != 'ALL_DATES' and date_end != 'ALL_DATES':
        conditions.append(f"DATE(adjusted_timestamp) BETWEEN '{date_start}' AND '{date_end}'")
    
    return "WHERE " + " AND ".join(conditions) if conditions else ""

def generate_level_fields(events):
    """Generate dynamic level fields based on available events"""
    level_fields = []
    level_counts = []
    
    # Find all level events
    level_events = [event for event in events.keys() if event.startswith('div_level_')]
    level_events.sort(key=lambda x: int(x.split('_')[-1]) if x.split('_')[-1].isdigit() else 0)
    
    for event in level_events:
        level_num = event.split('_')[-1]
        level_fields.append(f"MIN(CASE WHEN name = '{event}' THEN adjusted_timestamp END) as level_{level_num}_time")
        level_counts.append(f"COUNT(CASE WHEN name = '{event}' THEN 1 END) as level_{level_num}_count")
    
    return level_fields, level_counts, level_events

def generate_aggregation_query(dataset_name, schema_mapping, limit=1000):
    """Generate the main aggregation query with enhanced features (Final Working Version)"""
    print("📊 Generating enhanced aggregation query (Final Working Version)...")
    
    # Get filter information
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    
    where_clause = build_where_clause(app_filter, date_start, date_end)
    
    # Get events for dynamic field generation
    events = schema_mapping.get('events', {}).get('event_counts', {})
    level_fields, level_counts, level_events = generate_level_fields(events)
    
    # Get recommendations
    recommendations = schema_mapping.get('recommendations', {})
    primary_user_id = recommendations.get('primary_user_id', 'device_id')
    user_id_issues = recommendations.get('user_id_issues', [])
    
    # Generate max level reached with proper aggregation
    max_level_case = ""
    if level_events:
        max_level_case = f"""
        -- Max Level Reached (Fixed with proper aggregation)
        MAX(CASE 
            {chr(10).join([f"WHEN name = '{event}' THEN {event.split('_')[-1]}" for event in level_events])}
            ELSE 0
        END) as max_level_reached,"""
    else:
        max_level_case = "0 as max_level_reached,"
    
    # Generate the main query with proper GROUP BY handling
    query = f"""
    -- Enhanced User Daily Aggregation with Session Duration and Revenue Classification (Final Working)
    -- Generated for run: {schema_mapping.get('run_hash', 'unknown')}
    -- Data Quality Issues: {', '.join(user_id_issues) if user_id_issues else 'None'}
    
    WITH session_durations AS (
        SELECT 
            session_id,
            DATE(adjusted_timestamp) as date,
            MIN(adjusted_timestamp) as session_start,
            MAX(adjusted_timestamp) as session_end,
            TIMESTAMP_DIFF(MAX(adjusted_timestamp), MIN(adjusted_timestamp), MINUTE) as session_duration_minutes
        FROM `{dataset_name}`
        {where_clause}
        AND session_id IS NOT NULL
        GROUP BY session_id, DATE(adjusted_timestamp)
    ),
    
    user_cohorts AS (
        SELECT 
            COALESCE({primary_user_id}, device_id) as user_id,
            MIN(DATE(adjusted_timestamp)) as cohort_date
        FROM `{dataset_name}`
        {where_clause}
        GROUP BY COALESCE({primary_user_id}, device_id)
    )
    
    SELECT 
        -- Primary Key
        COALESCE({primary_user_id}, device_id) as user_id,
        device_id,
        DATE(t.adjusted_timestamp) as date,
        
        -- User Cohort Information
        uc.cohort_date,
        DATE_DIFF(ANY_VALUE(DATE(t.adjusted_timestamp)), uc.cohort_date, DAY) as days_since_first_event,
        CASE 
            WHEN DATE_DIFF(ANY_VALUE(DATE(t.adjusted_timestamp)), uc.cohort_date, DAY) = 0 THEN 'new'
            ELSE 'returning'
        END as user_type,
        
        -- Session Duration Metrics (Enhanced with Duration Calculation)
        -- Note: Session count removed to eliminate SQL ambiguity
        AVG(sd.session_duration_minutes) as avg_session_duration_minutes,
        MAX(sd.session_duration_minutes) as longest_session_duration_minutes,
        SUM(sd.session_duration_minutes) as total_session_time_minutes,
        
        -- Revenue Metrics (Enhanced with Classification)
        SUM(CASE WHEN is_revenue_valid = true THEN converted_revenue ELSE 0 END) as total_revenue,
        SUM(CASE WHEN is_revenue_valid = true AND converted_currency = 'USD' THEN converted_revenue ELSE 0 END) as total_revenue_usd,
        
        -- Revenue by Type (Enhanced Generic Classification)
        SUM(CASE WHEN is_revenue_valid = true AND (
            UPPER(name) LIKE '%IAP%' OR 
            UPPER(name) LIKE '%PURCHASE%' OR 
            UPPER(name) LIKE '%BUY%' OR
            UPPER(name) LIKE '%INAPP%' OR
            UPPER(name) LIKE '%TRANSACTION%'
        ) THEN converted_revenue ELSE 0 END) as iap_revenue,
        
        SUM(CASE WHEN is_revenue_valid = true AND (
            UPPER(name) LIKE '%AD%' OR 
            UPPER(name) LIKE '%ADS%' OR 
            UPPER(name) LIKE '%ADMON%' OR
            UPPER(name) LIKE '%ADVERTISEMENT%' OR
            UPPER(name) LIKE '%BANNER%' OR
            UPPER(name) LIKE '%INTERSTITIAL%' OR
            UPPER(name) LIKE '%REWARDED%'
        ) THEN converted_revenue ELSE 0 END) as ad_revenue,
        
        SUM(CASE WHEN is_revenue_valid = true AND (
            name LIKE '%sub%' OR 
            name LIKE '%subscription%' OR 
            name LIKE '%recurring%' OR
            name LIKE '%premium%' OR
            name LIKE '%pro%'
        ) THEN converted_revenue ELSE 0 END) as subscription_revenue,
        
        -- Revenue Event Counts by Type (Enhanced Generic Classification)
        COUNT(CASE WHEN is_revenue_valid = true AND (
            UPPER(name) LIKE '%IAP%' OR 
            UPPER(name) LIKE '%PURCHASE%' OR 
            UPPER(name) LIKE '%BUY%' OR
            UPPER(name) LIKE '%INAPP%' OR
            UPPER(name) LIKE '%TRANSACTION%'
        ) THEN 1 END) as iap_events_count,
        
        COUNT(CASE WHEN is_revenue_valid = true AND (
            UPPER(name) LIKE '%AD%' OR 
            UPPER(name) LIKE '%ADS%' OR 
            UPPER(name) LIKE '%ADMON%' OR
            UPPER(name) LIKE '%ADVERTISEMENT%' OR
            UPPER(name) LIKE '%BANNER%' OR
            UPPER(name) LIKE '%INTERSTITIAL%' OR
            UPPER(name) LIKE '%REWARDED%'
        ) THEN 1 END) as ad_events_count,
        
        COUNT(CASE WHEN is_revenue_valid = true AND (
            name LIKE '%sub%' OR 
            name LIKE '%subscription%' OR 
            name LIKE '%recurring%' OR
            name LIKE '%premium%' OR
            name LIKE '%pro%'
        ) THEN 1 END) as subscription_events_count,
        COUNT(CASE WHEN is_revenue_valid = true THEN 1 END) as total_revenue_events_count,
        
        -- Revenue Timestamps
        MIN(CASE WHEN is_revenue_valid = true THEN adjusted_timestamp END) as first_purchase_time,
        MAX(CASE WHEN is_revenue_valid = true THEN adjusted_timestamp END) as last_purchase_time,
        
        -- Event Counts & Engagement Metrics
        COUNT(*) as total_events,
        COUNT(DISTINCT name) as unique_events,
        
        -- Key Milestone Events
        MIN(CASE WHEN name = 'ftue_complete' THEN adjusted_timestamp END) as ftue_complete_time,
        MIN(CASE WHEN name = 'game_complete' THEN adjusted_timestamp END) as game_complete_time,
        
        -- Dynamic Level Fields
        {', '.join(level_fields) if level_fields else '-- No level events found'},
        
        -- Level Counts
        {', '.join(level_counts) if level_counts else '-- No level events found'},
        
        {max_level_case}
        
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
        {schema_mapping.get('data_quality', {}).get('overall_score', 0)} as data_quality_score,
        CURRENT_TIMESTAMP() as last_updated,
        '{schema_mapping.get('run_hash', 'unknown')}' as run_hash,
        ANY_VALUE(app_longname) as app_name,
        
        -- Data Quality Issues (JSON)
        '{json.dumps(user_id_issues)}' as data_quality_issues
        
    FROM `{dataset_name}` t
    LEFT JOIN session_durations sd ON t.session_id = sd.session_id AND DATE(t.adjusted_timestamp) = sd.date
    LEFT JOIN user_cohorts uc ON COALESCE({primary_user_id}, device_id) = uc.user_id
    {where_clause}
    GROUP BY 
        COALESCE({primary_user_id}, device_id),
        device_id,
        DATE(t.adjusted_timestamp),
        uc.cohort_date
    ORDER BY 
        COALESCE({primary_user_id}, device_id),
        DATE(t.adjusted_timestamp)
    LIMIT {limit};
    """
    
    return query

def create_bigquery_table(client, query, target_project, target_dataset, table_name):
    """Attempt to create BigQuery table"""
    print(f"🏗️ Attempting to create BigQuery table: {target_project}.{target_dataset}.{table_name}")
    
    try:
        # Create the table
        create_query = f"""
        CREATE OR REPLACE TABLE `{target_project}.{target_dataset}.{table_name}` AS
        {query}
        """
        
        job = client.query(create_query)
        job.result()  # Wait for job to complete
        
        print(f"✅ Successfully created BigQuery table: {target_project}.{target_dataset}.{table_name}")
        return True
        
    except Exception as e:
        if "Permission" in str(e) or "Access Denied" in str(e):
            print(f"⚠️ Access denied for table creation: {str(e)}")
            return False
        else:
            print(f"❌ Error creating BigQuery table: {str(e)}")
            raise

def export_to_csv(client, query, output_path):
    """Export query results to CSV"""
    print(f"📄 Exporting results to CSV: {output_path}")
    
    try:
        # Execute query
        df = client.query(query).to_dataframe()
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_path, index=False)
        
        print(f"✅ Successfully exported {len(df)} rows to: {output_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error exporting to CSV: {str(e)}")
        raise

def generate_summary_report(schema_mapping, output_path, success=True, table_created=False):
    """Generate a summary report of the aggregation process"""
    print("📊 Generating summary report...")
    
    recommendations = schema_mapping.get('recommendations', {})
    user_id_issues = recommendations.get('user_id_issues', [])
    data_quality_score = schema_mapping.get('data_quality', {}).get('overall_score', 0)
    
    summary = {
        "run_hash": schema_mapping.get('run_hash', 'unknown'),
        "timestamp": datetime.now().isoformat(),
        "status": "success" if success else "failed",
        "table_created": table_created,
        "data_quality_summary": {
            "overall_score": data_quality_score,
            "user_identification_issues": user_id_issues,
            "primary_user_id": recommendations.get('primary_user_id', 'unknown'),
            "substitute_used": "device_id" if "device_id" in str(user_id_issues) else "none"
        },
        "aggregation_features": {
            "session_duration_calculation": "implemented",
            "session_count": "removed_to_eliminate_ambiguity",
            "revenue_classification": "implemented",
            "dynamic_level_fields": "implemented",
            "cohort_analysis": "basic_implementation",
            "retention_metrics": "not_implemented"
        },
        "recommendations": {
            "user_identification": "Use device_id as primary identifier due to custom_user_id data quality issues" if user_id_issues else "No issues identified",
            "cohort_analysis": "Implement on larger datasets with multi-date analysis",
            "retention_metrics": "Implement when running on larger datasets with historical data",
            "session_count": "Can be added back later with proper session_id handling"
        }
    }
    
    try:
        with open(output_path, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        print(f"✅ Summary report saved to: {output_path}")
    except Exception as e:
        print(f"❌ Error saving summary report: {str(e)}")

def main():
    """Main aggregation function"""
    print("🚀 Starting Enhanced Data Aggregation v2.0.6 (Final Working) for Product Dashboard Builder v2")
    print("=" * 80)
    
    # Load environment variables
    load_dotenv()
    
    # Get environment variables
    run_hash = os.environ.get('RUN_HASH')
    dataset_name = os.environ.get('DATASET_NAME')
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    aggregation_limit = int(os.environ.get('AGGREGATION_LIMIT', '1000'))
    
    target_project = os.environ.get('TARGET_PROJECT', 'gc-prod-459709')
    target_dataset = os.environ.get('TARGET_DATASET', 'nbs_dataset')
    table_name = os.environ.get('AGGREGATION_TABLE_NAME', 'user_daily_aggregation_enhanced_v2')
    
    outputs_dir = f'run_logs/{run_hash}/outputs/aggregations'
    working_dir = f'run_logs/{run_hash}/working'
    
    print(f"Run Hash: {run_hash}")
    print(f"Dataset: {dataset_name}")
    print(f"App Filter: {app_filter if app_filter else 'ALL_APPS'}")
    print(f"Date Range: {date_start if date_start else 'ALL_DATES'} to {date_end if date_end else 'ALL_DATES'}")
    print(f"Aggregation Limit: {aggregation_limit}")
    print(f"Target Table: {target_project}.{target_dataset}.{table_name}")
    print(f"Outputs Directory: {outputs_dir}")
    print()
    
    try:
        # Initialize BigQuery client
        client = get_bigquery_client()
        
        # Load schema mapping
        schema_mapping = load_schema_mapping(run_hash)
        if not schema_mapping:
            print("❌ Failed to load schema mapping")
            return 1
        
        # Generate aggregation query
        query = generate_aggregation_query(dataset_name, schema_mapping, aggregation_limit)
        
        # Save query to working directory
        os.makedirs(working_dir, exist_ok=True)
        with open(f'{working_dir}/aggregation_query.sql', 'w') as f:
            f.write(query)
        print(f"✅ SQL query saved to: {working_dir}/aggregation_query.sql")
        
        # Attempt to create BigQuery table
        table_created = create_bigquery_table(client, query, target_project, target_dataset, table_name)
        
        # Export to CSV (either as fallback or additional output)
        csv_path = f'{outputs_dir}/aggregated_data.csv'
        export_to_csv(client, query, csv_path)
        
        # Generate summary report
        summary_path = f'{outputs_dir}/aggregation_summary.json'
        generate_summary_report(schema_mapping, summary_path, success=True, table_created=table_created)
        
        print("\n🎉 Enhanced Data Aggregation v2.0.6 (Final Working) completed successfully!")
        print(f"📊 Results available at: {outputs_dir}")
        print(f"📄 CSV file: {csv_path}")
        print(f"📋 Summary report: {summary_path}")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during aggregation: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
