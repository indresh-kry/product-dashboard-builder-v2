#!/usr/bin/env python3
"""
Enhanced Schema Discovery Script for Product Dashboard Builder v2
Handles missing app filters and date ranges gracefully with proper JSON serialization

This script provides comprehensive error handling for:
- Missing app filters (analyzes all apps)
- Missing date ranges (analyzes all dates)
- Invalid app names (shows available options)
- Invalid date ranges (shows available ranges)

Usage:
    python3 scripts/schema_discovery_enhanced.py

Environment Variables:
    APP_FILTER: App name to filter by (empty = all apps)
    DATE_START: Start date in YYYY-MM-DD format (empty = all dates)
    DATE_END: End date in YYYY-MM-DD format (empty = all dates)
    DATASET_NAME: BigQuery dataset name
    RUN_HASH: Unique run identifier
"""
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def get_bigquery_client():
    """Initialize BigQuery client with credentials"""
    credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=project_id)
    return client

def discover_schema(client, dataset_name):
    """Discover the complete schema of the dataset"""
    print("🔍 Discovering dataset schema...")
    
    # Get table schema
    table = client.get_table(dataset_name)
    schema_info = []
    
    for field in table.schema:
        schema_info.append({
            'name': field.name,
            'type': field.field_type,
            'mode': field.mode,
            'description': field.description or 'No description available'
        })
    
    print(f"✅ Found {len(schema_info)} columns in dataset")
    return schema_info

def get_available_apps(client, dataset_name):
    """Get list of available apps in the dataset"""
    print("📱 Discovering available apps in dataset...")
    
    query = f"""
    SELECT 
        app_longname,
        COUNT(*) as event_count,
        MIN(DATE(adjusted_timestamp)) as earliest_date,
        MAX(DATE(adjusted_timestamp)) as latest_date
    FROM `{dataset_name}`
    GROUP BY app_longname
    ORDER BY event_count DESC
    LIMIT 20
    """
    
    try:
        df = client.query(query).to_dataframe()
        print(f"✅ Found {len(df)} apps in dataset")
        
        # Convert dates to strings for JSON serialization
        if len(df) > 0:
            df['earliest_date'] = df['earliest_date'].astype(str)
            df['latest_date'] = df['latest_date'].astype(str)
        
        return df
    except Exception as e:
        print(f"❌ Error getting available apps: {str(e)}")
        return None

def get_available_date_range(client, dataset_name, app_filter=None):
    """Get available date range in the dataset"""
    print("📅 Discovering available date range...")
    
    where_clause = ""
    if app_filter:
        where_clause = f"WHERE app_longname = '{app_filter}'"
    
    query = f"""
    SELECT 
        MIN(DATE(adjusted_timestamp)) as earliest_date,
        MAX(DATE(adjusted_timestamp)) as latest_date,
        COUNT(*) as total_events
    FROM `{dataset_name}`
    {where_clause}
    """
    
    try:
        df = client.query(query).to_dataframe()
        if len(df) > 0:
            print(f"✅ Date range: {df.iloc[0]['earliest_date']} to {df.iloc[0]['latest_date']}")
            print(f"   Total events: {df.iloc[0]['total_events']:,}")
            
            # Convert dates to strings for JSON serialization
            df['earliest_date'] = df['earliest_date'].astype(str)
            df['latest_date'] = df['latest_date'].astype(str)
        
        return df
    except Exception as e:
        print(f"❌ Error getting date range: {str(e)}")
        return None

def build_where_clause(app_filter, date_start, date_end):
    """Build WHERE clause based on available filters"""
    conditions = []
    
    if app_filter and app_filter.strip():
        conditions.append(f"app_longname = '{app_filter}'")
        print(f"🎯 App Filter: {app_filter}")
    else:
        print("🎯 App Filter: None (analyzing all apps)")
    
    if date_start and date_end and date_start.strip() and date_end.strip():
        conditions.append(f"DATE(adjusted_timestamp) BETWEEN '{date_start}' AND '{date_end}'")
        print(f"📅 Date Range: {date_start} to {date_end}")
    else:
        print("📅 Date Range: None (analyzing all available dates)")
    
    if conditions:
        return "WHERE " + " AND ".join(conditions)
    else:
        return ""

def sample_data(client, dataset_name, limit=1000):
    """Get sample data from the dataset with optional filters"""
    print("📊 Sampling data from dataset...")
    
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    
    # Build WHERE clause
    where_clause = build_where_clause(app_filter, date_start, date_end)
    
    query = f"""
    SELECT *
    FROM `{dataset_name}`
    {where_clause}
    LIMIT {limit}
    """
    
    try:
        df = client.query(query).to_dataframe()
        print(f"✅ Sampled {len(df)} rows from dataset")
        return df
    except Exception as e:
        print(f"❌ Error sampling data: {str(e)}")
        return None

def analyze_event_taxonomy(client, dataset_name):
    """Analyze event taxonomy and categorization with optional filters"""
    print("📋 Analyzing event taxonomy...")
    
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    
    # Build WHERE clause
    where_clause = build_where_clause(app_filter, date_start, date_end)
    
    query = f"""
    SELECT
        name as event_name,
        COUNT(*) as event_count,
        COUNT(DISTINCT custom_user_id) as unique_users,
        COUNT(DISTINCT device_id) as unique_devices,
        MIN(adjusted_timestamp) as first_seen,
        MAX(adjusted_timestamp) as last_seen,
        COUNT(DISTINCT DATE(adjusted_timestamp)) as active_days
    FROM `{dataset_name}`
    {where_clause}
    GROUP BY name
    ORDER BY event_count DESC
    """
    
    try:
        df = client.query(query).to_dataframe()
        print(f"✅ Found {len(df)} unique events")
        
        # Convert timestamps to strings for JSON serialization
        if len(df) > 0:
            df['first_seen'] = df['first_seen'].astype(str)
            df['last_seen'] = df['last_seen'].astype(str)
        
        return df
        
    except Exception as e:
        print(f"❌ Error analyzing event taxonomy: {str(e)}")
        return None

def identify_user_columns(client, dataset_name, sample_df):
    """Identify potential user identification columns"""
    print("👤 Identifying user identification columns...")
    
    if sample_df is None or len(sample_df) == 0:
        print("⚠️  No sample data available for user column analysis")
        return []
    
    user_candidates = []
    
    # Check common user ID column patterns
    user_patterns = ['user_id', 'custom_user_id', 'player_id', 'customer_id', 'client_id']
    
    for col in sample_df.columns:
        col_lower = col.lower()
        if any(pattern in col_lower for pattern in user_patterns):
            # Analyze the column
            unique_count = int(sample_df[col].nunique())
            null_count = int(sample_df[col].isnull().sum())
            total_count = len(sample_df)
            
            user_candidates.append({
                'column_name': col,
                'unique_values': unique_count,
                'null_count': null_count,
                'null_percentage': round((null_count / total_count) * 100, 2) if total_count > 0 else 0,
                'data_type': str(sample_df[col].dtype),
                'sample_values': [str(x) for x in sample_df[col].dropna().head(3).tolist()]
            })
    
    # Sort by uniqueness (higher is better for user IDs)
    user_candidates.sort(key=lambda x: x['unique_values'], reverse=True)
    
    print(f"✅ Found {len(user_candidates)} potential user ID columns")
    return user_candidates

def analyze_revenue_columns(client, dataset_name, sample_df):
    """Analyze revenue-related columns"""
    print("💰 Analyzing revenue columns...")
    
    if sample_df is None or len(sample_df) == 0:
        print("⚠️  No sample data available for revenue analysis")
        return []
    
    revenue_columns = []
    revenue_patterns = ['revenue', 'price', 'amount', 'cost', 'value', 'currency']
    
    for col in sample_df.columns:
        col_lower = col.lower()
        if any(pattern in col_lower for pattern in revenue_patterns):
            # Analyze the column
            if sample_df[col].dtype in ['int64', 'float64']:
                non_zero_count = int((sample_df[col] > 0).sum())
                min_val = float(sample_df[col].min()) if pd.notna(sample_df[col].min()) else None
                max_val = float(sample_df[col].max()) if pd.notna(sample_df[col].max()) else None
            else:
                non_zero_count = 0
                min_val = None
                max_val = None
                
            null_count = int(sample_df[col].isnull().sum())
            total_count = len(sample_df)
            
            revenue_columns.append({
                'column_name': col,
                'non_zero_count': non_zero_count,
                'null_count': null_count,
                'null_percentage': round((null_count / total_count) * 100, 2) if total_count > 0 else 0,
                'data_type': str(sample_df[col].dtype),
                'min_value': min_val,
                'max_value': max_val,
                'sample_values': [str(x) for x in sample_df[col].dropna().head(3).tolist()]
            })
    
    print(f"✅ Found {len(revenue_columns)} potential revenue columns")
    return revenue_columns

def assess_data_quality(client, dataset_name, sample_df):
    """Assess data quality metrics"""
    print("🔍 Assessing data quality...")
    
    if sample_df is None or len(sample_df) == 0:
        print("⚠️  No sample data available for quality assessment")
        return {}
    
    quality_metrics = {}
    
    for col in sample_df.columns:
        null_count = int(sample_df[col].isnull().sum())
        total_count = len(sample_df)
        null_percentage = round((null_count / total_count) * 100, 2) if total_count > 0 else 0
        
        quality_metrics[col] = {
            'null_count': null_count,
            'null_percentage': null_percentage,
            'data_type': str(sample_df[col].dtype),
            'unique_values': int(sample_df[col].nunique()),
            'duplicate_percentage': round(((total_count - sample_df[col].nunique()) / total_count) * 100, 2) if total_count > 0 else 0
        }
    
    print(f"✅ Assessed data quality for {len(quality_metrics)} columns")
    return quality_metrics

def create_schema_mapping(schema_info, user_candidates, revenue_columns, event_taxonomy_df, quality_metrics, available_apps_df, date_range_df):
    """Create the master schema mapping"""
    print("📝 Creating master schema mapping...")
    
    # Determine best user ID column
    best_user_column = user_candidates[0]['column_name'] if user_candidates else 'custom_user_id'
    
    # Determine revenue columns
    revenue_col = None
    revenue_validation_col = None
    revenue_currency_col = None
    
    for col in revenue_columns:
        if 'converted_revenue' in col['column_name'].lower():
            revenue_col = col['column_name']
        elif 'is_revenue' in col['column_name'].lower():
            revenue_validation_col = col['column_name']
        elif 'currency' in col['column_name'].lower():
            revenue_currency_col = col['column_name']
    
    # Get filter information
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    
    # Calculate data quality score
    if quality_metrics:
        data_quality_score = round(100 - sum(q['null_percentage'] for q in quality_metrics.values()) / len(quality_metrics), 2)
    else:
        data_quality_score = 0
    
    # Prepare available apps data for JSON serialization
    available_apps_data = []
    if available_apps_df is not None and len(available_apps_df) > 0:
        available_apps_data = available_apps_df.to_dict('records')
    
    # Prepare date range data for JSON serialization
    date_range_data = {}
    if date_range_df is not None and len(date_range_df) > 0:
        date_range_data = date_range_df.to_dict('records')[0]
    
    schema_mapping = {
        'version': '1.0',
        'run_hash': os.environ.get('RUN_HASH'),
        'generated_at': datetime.now().isoformat(),
        'table': os.environ.get('DATASET_NAME'),
        'filters': {
            'app_filter': app_filter if app_filter else 'ALL_APPS',
            'date_start': date_start if date_start else 'ALL_DATES',
            'date_end': date_end if date_end else 'ALL_DATES'
        },
        'data_quality_score': data_quality_score,
        'user_identification': {
            'primary_field': best_user_column,
            'candidates': user_candidates,
            'rules': {
                'use_primary': f"Use {best_user_column} for user identification",
                'fallback': "Use device_id if primary field is null"
            }
        },
        'revenue_calculation': {
            'revenue_field': revenue_col or 'converted_revenue',
            'validation_field': revenue_validation_col or 'is_revenue_valid',
            'currency_field': revenue_currency_col or 'converted_currency',
            'columns_analyzed': revenue_columns
        },
        'event_taxonomy': {
            'total_events': len(event_taxonomy_df) if event_taxonomy_df is not None else 0,
            'sample_events': event_taxonomy_df['event_name'].head(10).tolist() if event_taxonomy_df is not None else []
        },
        'data_quality': quality_metrics,
        'schema_info': schema_info,
        'available_apps': available_apps_data,
        'available_date_range': date_range_data
    }
    
    print("✅ Master schema mapping created")
    return schema_mapping

def main():
    """Main schema discovery function"""
    print("🚀 Starting Enhanced Schema Discovery for Product Dashboard Builder v2")
    print("=" * 80)
    
    # Get environment variables
    run_hash = os.environ.get('RUN_HASH')
    dataset_name = os.environ.get('DATASET_NAME')
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    outputs_dir = f'run_logs/{run_hash}/outputs/schema'
    
    print(f"Run Hash: {run_hash}")
    print(f"Dataset: {dataset_name}")
    print(f"App Filter: {app_filter if app_filter else 'ALL_APPS'}")
    print(f"Date Range: {date_start if date_start else 'ALL_DATES'} to {date_end if date_end else 'ALL_DATES'}")
    print(f"Outputs Directory: {outputs_dir}")
    print()
    
    try:
        # Initialize BigQuery client
        client = get_bigquery_client()
        
        # 1. Discover schema
        schema_info = discover_schema(client, dataset_name)
        
        # 2. Get available apps and date range
        available_apps_df = get_available_apps(client, dataset_name)
        date_range_df = get_available_date_range(client, dataset_name, app_filter if app_filter else None)
        
        # 3. Sample data with optional filters
        sample_df = sample_data(client, dataset_name)
        if sample_df is None or len(sample_df) == 0:
            print("❌ No data found with the specified filters. Please check your app filter and date range.")
            print("💡 Available apps and date ranges have been saved for reference.")
            
            # Still create a minimal schema mapping
            schema_mapping = {
                'version': '1.0',
                'run_hash': run_hash,
                'generated_at': datetime.now().isoformat(),
                'table': dataset_name,
                'filters': {
                    'app_filter': app_filter if app_filter else 'ALL_APPS',
                    'date_start': date_start if date_start else 'ALL_DATES',
                    'date_end': date_end if date_end else 'ALL_DATES'
                },
                'error': 'No data found with specified filters',
                'available_apps': available_apps_df.to_dict('records') if available_apps_df is not None else [],
                'available_date_range': date_range_df.to_dict('records')[0] if date_range_df is not None and len(date_range_df) > 0 else {}
            }
            
            # Save minimal outputs
            with open(f'{outputs_dir}/schema_mapping.json', 'w') as f:
                json.dump(schema_mapping, f, indent=2)
            
            if available_apps_df is not None:
                available_apps_df.to_csv(f'{outputs_dir}/available_apps.csv', index=False)
            
            print("✅ Minimal schema mapping saved with available apps and date ranges")
            return 1
        
        # 4. Analyze event taxonomy with optional filters
        event_taxonomy_df = analyze_event_taxonomy(client, dataset_name)
        
        # 5. Identify user columns
        user_candidates = identify_user_columns(client, dataset_name, sample_df)
        
        # 6. Analyze revenue columns
        revenue_columns = analyze_revenue_columns(client, dataset_name, sample_df)
        
        # 7. Assess data quality
        quality_metrics = assess_data_quality(client, dataset_name, sample_df)
        
        # 8. Create master schema mapping
        schema_mapping = create_schema_mapping(
            schema_info, user_candidates, revenue_columns, 
            event_taxonomy_df, quality_metrics, available_apps_df, date_range_df
        )
        
        # Save outputs
        print("\n💾 Saving outputs...")
        
        # Save column definitions
        with open(f'{outputs_dir}/column_definitions.json', 'w') as f:
            json.dump(schema_info, f, indent=2)
        
        # Save event taxonomy
        if event_taxonomy_df is not None:
            event_taxonomy_df.to_csv(f'{outputs_dir}/event_taxonomy.csv', index=False)
        
        # Save available apps
        if available_apps_df is not None:
            available_apps_df.to_csv(f'{outputs_dir}/available_apps.csv', index=False)
        
        # Save user identification analysis
        with open(f'{outputs_dir}/user_identification_analysis.json', 'w') as f:
            json.dump(user_candidates, f, indent=2)
        
        # Save revenue analysis
        with open(f'{outputs_dir}/revenue_analysis.json', 'w') as f:
            json.dump(revenue_columns, f, indent=2)
        
        # Save data quality report
        with open(f'{outputs_dir}/data_quality_report.json', 'w') as f:
            json.dump(quality_metrics, f, indent=2)
        
        # Save master schema mapping
        with open(f'{outputs_dir}/schema_mapping.json', 'w') as f:
            json.dump(schema_mapping, f, indent=2)
        
        # Create data quality report markdown
        with open(f'{outputs_dir}/data_quality_report.md', 'w') as f:
            f.write(f"# Data Quality Report - Run {run_hash}\n\n")
            f.write(f"**Dataset**: {dataset_name}\n")
            f.write(f"**App Filter**: {app_filter if app_filter else 'ALL_APPS'}\n")
            f.write(f"**Date Range**: {date_start if date_start else 'ALL_DATES'} to {date_end if date_end else 'ALL_DATES'}\n")
            f.write(f"**Generated**: {datetime.now().isoformat()}\n\n")
            f.write("## Summary\n\n")
            f.write(f"- **Total Columns**: {len(schema_info)}\n")
            f.write(f"- **Data Quality Score**: {schema_mapping['data_quality_score']}%\n")
            f.write(f"- **Primary User ID Column**: {schema_mapping['user_identification']['primary_field']}\n")
            f.write(f"- **Revenue Column**: {schema_mapping['revenue_calculation']['revenue_field']}\n")
            f.write(f"- **Total Events**: {schema_mapping['event_taxonomy']['total_events']}\n\n")
            
            if available_apps_df is not None and len(available_apps_df) > 0:
                f.write("## Available Apps\n\n")
                f.write("| App Name | Event Count | Earliest Date | Latest Date |\n")
                f.write("|----------|-------------|---------------|-------------|\n")
                for _, row in available_apps_df.head(10).iterrows():
                    f.write(f"| {row['app_longname']} | {row['event_count']:,} | {row['earliest_date']} | {row['latest_date']} |\n")
                f.write("\n")
            
            if quality_metrics:
                f.write("## Column Quality Metrics\n\n")
                f.write("| Column | Null % | Data Type | Unique Values | Duplicate % |\n")
                f.write("|--------|--------|-----------|---------------|-------------|\n")
                for col, metrics in quality_metrics.items():
                    f.write(f"| {col} | {metrics['null_percentage']}% | {metrics['data_type']} | {metrics['unique_values']} | {metrics['duplicate_percentage']}% |\n")
        
        print("✅ All outputs saved successfully!")
        print(f"📁 Outputs saved to: {outputs_dir}")
        print(f"📄 Master schema mapping: {outputs_dir}/schema_mapping.json")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error during schema discovery: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
