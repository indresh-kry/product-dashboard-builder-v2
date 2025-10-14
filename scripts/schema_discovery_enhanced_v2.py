#!/usr/bin/env python3
"""
Enhanced Schema Discovery Script with Session Duration and Revenue Classification
Version: 2.0.0
Last Updated: 2025-10-14

Changelog:
- v2.0.0 (2025-10-14): Added session duration calculation fields and revenue classification
- v1.0.0 (2025-10-14): Initial version with raw data output capability

Environment Variables:
- RUN_HASH: Unique identifier for the current run
- DATASET_NAME: BigQuery dataset name
- APP_FILTER: Filter by app name (optional)
- DATE_START: Start date for filtering (optional)
- DATE_END: End date for filtering (optional)
- RAW_DATA_LIMIT: Number of raw rows to sample (default: 10000)
- GOOGLE_CLOUD_PROJECT: Google Cloud project ID
- GOOGLE_APPLICATION_CREDENTIALS: Path to service account credentials

Dependencies:
- google-cloud-bigquery: BigQuery client library
- google-oauth2: OAuth2 authentication
- pandas: Data manipulation and analysis
- json: JSON serialization
"""
import os
import json
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

def get_bigquery_client():
    """Initialize BigQuery client with credentials"""
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=project_id)
    return client

def build_where_clause(app_filter, date_start, date_end):
    """Build WHERE clause for SQL queries"""
    conditions = []
    
    if app_filter and app_filter.strip() and app_filter != 'ALL_APPS':
        conditions.append(f"app_longname = '{app_filter}'")
    
    if date_start and date_end and date_start.strip() and date_end.strip() and date_start != 'ALL_DATES' and date_end != 'ALL_DATES':
        conditions.append(f"DATE(adjusted_timestamp) BETWEEN '{date_start}' AND '{date_end}'")
    
    return "WHERE " + " AND ".join(conditions) if conditions else ""

def get_available_apps(client, dataset_name):
    """Get list of available apps in the dataset"""
    query = f"""
    SELECT 
        app_longname,
        COUNT(*) as event_count,
        MIN(DATE(adjusted_timestamp)) as min_date,
        MAX(DATE(adjusted_timestamp)) as max_date
    FROM `{dataset_name}`
    GROUP BY app_longname
    ORDER BY event_count DESC
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        print(f"❌ Error getting available apps: {str(e)}")
        return None

def get_available_date_range(client, dataset_name, app_filter=None):
    """Get available date range for the dataset or specific app"""
    where_clause = build_where_clause(app_filter, None, None)
    
    query = f"""
    SELECT 
        MIN(DATE(adjusted_timestamp)) as min_date,
        MAX(DATE(adjusted_timestamp)) as max_date,
        COUNT(*) as total_events
    FROM `{dataset_name}`
    {where_clause}
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df.iloc[0] if len(df) > 0 else None
    except Exception as e:
        print(f"❌ Error getting date range: {str(e)}")
        return None

def discover_schema(client, dataset_name):
    """Discover the schema of the dataset"""
    print("🔍 Discovering dataset schema...")
    
    try:
        # Get table schema
        table_ref = client.get_table(dataset_name)
        schema = []
        
        for field in table_ref.schema:
            schema.append({
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description or "No description available"
            })
        
        print(f"✅ Found {len(schema)} columns in dataset")
        return schema
    except Exception as e:
        print(f"❌ Error discovering schema: {str(e)}")
        return []

def sample_data(client, dataset_name, app_filter, date_start, date_end, limit=10000):
    """Sample data from the dataset"""
    print("📊 Sampling data from dataset...")
    
    where_clause = build_where_clause(app_filter, date_start, date_end)
    
    query = f"""
    SELECT *
    FROM `{dataset_name}`
    {where_clause}
    ORDER BY adjusted_timestamp DESC
    LIMIT {limit}
    """
    
    try:
        df = client.query(query).to_dataframe()
        print(f"✅ Sampled {len(df)} rows from dataset")
        return df
    except Exception as e:
        print(f"❌ Error sampling data: {str(e)}")
        return None

def analyze_events(df):
    """Analyze event taxonomy from sampled data"""
    print("📋 Analyzing event taxonomy...")
    
    if df is None or len(df) == 0:
        return {}
    
    # Get unique events
    unique_events = df['name'].value_counts().to_dict()
    
    print(f"✅ Found {len(unique_events)} unique events")
    return unique_events

def identify_user_columns(df):
    """Identify potential user identification columns with data quality assessment"""
    print("👤 Identifying user identification columns...")
    
    if df is None or len(df) == 0:
        return {}
    
    user_columns = {}
    
    # Check for common user ID columns
    potential_user_cols = ['custom_user_id', 'user_id', 'device_id', 'gaid', 'idfa']
    
    for col in potential_user_cols:
        if col in df.columns:
            non_null_count = df[col].notna().sum()
            unique_count = df[col].nunique()
            null_percentage = (len(df) - non_null_count) / len(df) * 100 if len(df) > 0 else 0
            
            # Data quality assessment
            quality_issues = []
            if unique_count == 1:
                quality_issues.append("Only 1 unique value - not suitable as user identifier")
            elif unique_count < len(df) * 0.1:  # Less than 10% unique
                quality_issues.append("Low uniqueness - potential data quality issue")
            
            user_columns[col] = {
                "non_null_count": int(non_null_count),
                "unique_count": int(unique_count),
                "null_percentage": float(null_percentage),
                "quality_issues": quality_issues,
                "recommended_for_aggregation": unique_count > 1 and null_percentage < 50
            }
    
    print(f"✅ Found {len(user_columns)} potential user ID columns")
    return user_columns

def analyze_revenue_columns(df):
    """Analyze revenue-related columns with classification logic"""
    print("💰 Analyzing revenue columns...")
    
    if df is None or len(df) == 0:
        return {}
    
    revenue_columns = {}
    
    # Check for revenue-related columns
    potential_revenue_cols = ['converted_revenue', 'revenue', 'received_revenue', 'converted_currency', 'is_revenue_valid', 'received_revenue_event']
    
    for col in potential_revenue_cols:
        if col in df.columns:
            non_null_count = df[col].notna().sum()
            unique_count = df[col].nunique()
            null_percentage = (len(df) - non_null_count) / len(df) * 100 if len(df) > 0 else 0
            
            revenue_columns[col] = {
                "non_null_count": int(non_null_count),
                "unique_count": int(unique_count),
                "null_percentage": float(null_percentage)
            }
    
    # Analyze revenue event classification
    if 'received_revenue_event' in df.columns:
        revenue_events = df['received_revenue_event'].value_counts().to_dict()
        revenue_classification = {}
        
        for event, count in revenue_events.items():
            if pd.isna(event):
                continue
            event_str = str(event).lower()
            if 'iap' in event_str or 'purchase' in event_str:
                revenue_classification[event] = 'iap'
            elif 'ad' in event_str or 'admon' in event_str:
                revenue_classification[event] = 'ad'
            elif 'subscription' in event_str or 'sub' in event_str:
                revenue_classification[event] = 'subscription'
            else:
                revenue_classification[event] = 'other'
        
        revenue_columns['revenue_event_classification'] = revenue_classification
    
    print(f"✅ Found {len(revenue_columns)} potential revenue columns")
    return revenue_columns

def analyze_session_fields(df):
    """Analyze session-related fields for duration calculation"""
    print("🕐 Analyzing session fields for duration calculation...")
    
    if df is None or len(df) == 0:
        return {}
    
    session_analysis = {}
    
    # Check for session-related columns
    session_cols = ['session_id', 'adjusted_timestamp']
    
    for col in session_cols:
        if col in df.columns:
            non_null_count = df[col].notna().sum()
            unique_count = df[col].nunique()
            null_percentage = (len(df) - non_null_count) / len(df) * 100 if len(df) > 0 else 0
            
            session_analysis[col] = {
                "non_null_count": int(non_null_count),
                "unique_count": int(unique_count),
                "null_percentage": float(null_percentage)
            }
    
    # Analyze session events
    if 'name' in df.columns:
        session_events = df[df['name'].str.contains('SESSION', case=False, na=False)]['name'].value_counts().to_dict()
        session_analysis['session_events'] = session_events
    
    # Session duration calculation strategy
    session_analysis['duration_calculation_strategy'] = {
        "method": "timestamp_based",
        "description": "Calculate session duration using MIN/MAX adjusted_timestamp per session_id per date",
        "formula": "session_duration = MAX(adjusted_timestamp) - MIN(adjusted_timestamp) WHERE session_id = X AND DATE(adjusted_timestamp) = Y",
        "fallback": "Use 30-minute default timeout if no session end detected"
    }
    
    print("✅ Session analysis completed")
    return session_analysis

def assess_data_quality(df):
    """Assess data quality for all columns"""
    print("🔍 Assessing data quality...")
    
    if df is None or len(df) == 0:
        return {}
    
    quality_metrics = {}
    
    for col in df.columns:
        non_null_count = df[col].notna().sum()
        unique_count = df[col].nunique()
        quality_metrics[col] = {
            "non_null_count": int(non_null_count),
            "unique_count": int(unique_count),
            "null_percentage": float((len(df) - non_null_count) / len(df) * 100) if len(df) > 0 else 0,
            "data_type": str(df[col].dtype)
        }
    
    print(f"✅ Assessed data quality for {len(quality_metrics)} columns")
    return quality_metrics

def create_schema_mapping(schema, events, user_columns, revenue_columns, session_analysis, quality_metrics, app_filter, date_start, date_end, run_hash):
    """Create master schema mapping with enhanced analysis"""
    print("📝 Creating master schema mapping...")
    
    # Calculate overall data quality score
    total_columns = len(quality_metrics)
    high_quality_columns = sum(1 for col, metrics in quality_metrics.items() if metrics['null_percentage'] < 10)
    data_quality_score = (high_quality_columns / total_columns * 100) if total_columns > 0 else 0
    
    # Identify primary user ID
    primary_user_id = None
    user_id_issues = []
    
    for col, metrics in user_columns.items():
        if metrics.get('recommended_for_aggregation', False):
            primary_user_id = col
            break
    
    if not primary_user_id:
        # Fallback to device_id if available
        if 'device_id' in user_columns:
            primary_user_id = 'device_id'
            user_id_issues.append("Using device_id as primary user identifier due to custom_user_id data quality issues")
        else:
            user_id_issues.append("No suitable user identifier found")
    
    schema_mapping = {
        "run_hash": run_hash,
        "timestamp": datetime.now().isoformat(),
        "dataset_name": os.environ.get('DATASET_NAME'),
        "filters": {
            "app_filter": app_filter,
            "date_start": date_start,
            "date_end": date_end
        },
        "schema": {
            "total_columns": len(schema),
            "columns": schema
        },
        "events": {
            "total_unique_events": len(events),
            "event_counts": events
        },
        "user_identification": user_columns,
        "revenue_analysis": revenue_columns,
        "session_analysis": session_analysis,
        "data_quality": {
            "overall_score": round(data_quality_score, 2),
            "column_metrics": quality_metrics
        },
        "recommendations": {
            "primary_user_id": primary_user_id,
            "primary_revenue_column": "converted_revenue" if "converted_revenue" in revenue_columns else "revenue",
            "timestamp_column": "adjusted_timestamp",
            "session_duration_calculation": session_analysis.get('duration_calculation_strategy', {}),
            "user_id_issues": user_id_issues
        }
    }
    
    print("✅ Master schema mapping created")
    return schema_mapping

def save_outputs(schema_mapping, df, outputs_dir):
    """Save all outputs to files"""
    print("💾 Saving outputs...")
    
    try:
        # Save schema mapping
        with open(f'{outputs_dir}/schema_mapping.json', 'w') as f:
            json.dump(schema_mapping, f, indent=2, default=str)
        
        # Save column definitions
        with open(f'{outputs_dir}/column_definitions.json', 'w') as f:
            json.dump(schema_mapping['schema']['columns'], f, indent=2, default=str)
        
        # Save event taxonomy
        with open(f'{outputs_dir}/event_taxonomy.json', 'w') as f:
            json.dump(schema_mapping['events'], f, indent=2, default=str)
        
        # Save user identification analysis
        with open(f'{outputs_dir}/user_identification.json', 'w') as f:
            json.dump(schema_mapping['user_identification'], f, indent=2, default=str)
        
        # Save revenue analysis
        with open(f'{outputs_dir}/revenue_analysis.json', 'w') as f:
            json.dump(schema_mapping['revenue_analysis'], f, indent=2, default=str)
        
        # Save session analysis
        with open(f'{outputs_dir}/session_analysis.json', 'w') as f:
            json.dump(schema_mapping['session_analysis'], f, indent=2, default=str)
        
        # Save data quality assessment
        with open(f'{outputs_dir}/data_quality_assessment.json', 'w') as f:
            json.dump(schema_mapping['data_quality'], f, indent=2, default=str)
        
        # Save raw data if available
        if df is not None and len(df) > 0:
            raw_data_file = f'{outputs_dir}/../raw_data/sampled_raw_data.csv'
            os.makedirs(os.path.dirname(raw_data_file), exist_ok=True)
            df.to_csv(raw_data_file, index=False)
            print(f"✅ Raw data saved to: {raw_data_file}")
        
        print("✅ All outputs saved successfully!")
        print(f"📁 Outputs saved to: {outputs_dir}")
        print(f"📄 Master schema mapping: {outputs_dir}/schema_mapping.json")
        
    except Exception as e:
        print(f"❌ Error saving outputs: {str(e)}")
        raise

def main():
    """Main schema discovery function"""
    print("🚀 Starting Enhanced Schema Discovery v2.0 for Product Dashboard Builder v2")
    print("=" * 80)
    
    # Get environment variables
    run_hash = os.environ.get('RUN_HASH')
    dataset_name = os.environ.get('DATASET_NAME')
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    raw_data_limit = int(os.environ.get('RAW_DATA_LIMIT', '10000'))
    
    outputs_dir = f'run_logs/{run_hash}/outputs/schema'
    
    print(f"Run Hash: {run_hash}")
    print(f"Dataset: {dataset_name}")
    print(f"App Filter: {app_filter if app_filter else 'ALL_APPS'}")
    print(f"Date Range: {date_start if date_start else 'ALL_DATES'} to {date_end if date_end else 'ALL_DATES'}")
    print(f"Raw Data Limit: {raw_data_limit}")
    print(f"Outputs Directory: {outputs_dir}")
    print()
    
    try:
        # Initialize BigQuery client
        client = get_bigquery_client()
        
        # Discover schema
        schema = discover_schema(client, dataset_name)
        
        # Get available apps
        print("📱 Discovering available apps in dataset...")
        apps_df = get_available_apps(client, dataset_name)
        if apps_df is not None:
            print(f"✅ Found {len(apps_df)} apps in dataset")
        
        # Get available date range
        print("📅 Discovering available date range...")
        date_range = get_available_date_range(client, dataset_name, app_filter)
        if date_range is not None:
            print(f"✅ Date range: {date_range['min_date']} to {date_range['max_date']}")
            print(f"   Total events: {date_range['total_events']:,}")
        
        # Sample data
        df = sample_data(client, dataset_name, app_filter, date_start, date_end, raw_data_limit)
        
        # Analyze events
        events = analyze_events(df)
        
        # Identify user columns
        user_columns = identify_user_columns(df)
        
        # Analyze revenue columns
        revenue_columns = analyze_revenue_columns(df)
        
        # Analyze session fields
        session_analysis = analyze_session_fields(df)
        
        # Assess data quality
        quality_metrics = assess_data_quality(df)
        
        # Create schema mapping
        schema_mapping = create_schema_mapping(schema, events, user_columns, revenue_columns, session_analysis, quality_metrics, app_filter, date_start, date_end, run_hash)
        
        # Save outputs
        save_outputs(schema_mapping, df, outputs_dir)
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during schema discovery: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
