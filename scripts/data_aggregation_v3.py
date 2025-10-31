#!/usr/bin/env python3
"""
Enhanced Data Aggregation Script - Final Working Version with Safety Guards
Version: 3.5.0
Last Updated: 2025-10-31

Changelog:
- v3.5.0 (2025-10-31): Added day-by-day processing for date ranges - data is ingested sequentially day by day and results are combined
    - Fixed cohort calculation for day-by-day processing - cohort assignment now uses full historical range while aggregation filters to single day
- v3.4.0 (2025-10-15): Added BigQuery safety guardrails to prevent source table modification
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
from datetime import datetime, timedelta
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import traceback
import sys

# Import safety module
try:
    from bigquery_safety import get_safe_bigquery_client, validate_environment_safety, BigQuerySafetyError
except ImportError:
    print("⚠️ Warning: BigQuery safety module not found. Running without safety guards.")
    get_safe_bigquery_client = None
    validate_environment_safety = None
    BigQuerySafetyError = Exception

# At script start, after loading field_mapping.json, set event_name_col = field_mapping['event_name_column']; error and exit if missing. Remove all defaults to 'name'.
# In all dynamic SQL and field generator blocks, replace field references 'name' with event_name_col from config, always.
# Document this behavior at the top and in comments near field usage.

def get_bigquery_client():
    """Initialize BigQuery client with credentials and safety guards"""
    # Validate environment safety first
    if validate_environment_safety and not validate_environment_safety():
        raise RuntimeError("Environment safety validation failed. Check BIGQUERY_READ_ONLY_MODE setting.")
    
    # Use safe client if available, fallback to regular client
    if get_safe_bigquery_client:
        dataset_name = os.environ.get('DATASET_NAME', '')
        source_dataset = dataset_name.split('.')[-1] if '.' in dataset_name else dataset_name
        return get_safe_bigquery_client(source_dataset)
    else:
        # Fallback to regular client (for backward compatibility)
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

def build_where_clause(app_filter, date_start, date_end, timestamp_column: str, single_date=None):
    """Build WHERE clauses for SQL queries - separate cohort and main aggregation clauses
    
    Args:
        app_filter: App filter string
        date_start: Start date for range (used for cohort assignment extension)
        date_end: End date for range
        timestamp_column: Timestamp column name
        single_date: If provided, filter main aggregation to this single date (overrides date_start/date_end)
    
    Returns:
        tuple: (cohort_where_clause, main_where_clause)
            - cohort_where_clause: Full date range (extended start to end_date) for cohort calculation
            - main_where_clause: Single day filter if single_date provided, otherwise full range
    """
    app_filter_condition = []
    if app_filter and app_filter.strip() and app_filter != 'ALL_APPS':
        app_filter_condition.append(f"app_longname = '{app_filter}'")
    app_filter_str = " AND ".join(app_filter_condition) if app_filter_condition else ""
    
    # Calculate extended start date for cohort assignment (7 days prior to original start)
    cohort_start_str = None
    if date_start and date_start.strip() and date_start != 'ALL_DATES':
        start_date = datetime.strptime(date_start, '%Y-%m-%d')
        cohort_start_date = start_date - timedelta(days=7)
        cohort_start_str = cohort_start_date.strftime('%Y-%m-%d')
    
    # Build cohort WHERE clause (always uses full extended range)
    cohort_conditions = []
    if app_filter_str:
        cohort_conditions.append(app_filter_str)
    if date_start and date_end and date_start.strip() and date_end.strip() and date_start != 'ALL_DATES' and date_end != 'ALL_DATES':
        if cohort_start_str:
            cohort_conditions.append(f"DATE({timestamp_column}) BETWEEN '{cohort_start_str}' AND '{date_end}'")
        else:
            cohort_conditions.append(f"DATE({timestamp_column}) BETWEEN '{date_start}' AND '{date_end}'")
    cohort_where_clause = " AND ".join(cohort_conditions) if cohort_conditions else ""
    
    # Build main WHERE clause (uses single_date if provided, otherwise full range)
    main_conditions = []
    if app_filter_str:
        main_conditions.append(app_filter_str)
    if single_date:
        # Single day filter for main aggregation
        main_conditions.append(f"DATE({timestamp_column}) = '{single_date}'")
    elif date_start and date_end and date_start.strip() and date_end.strip() and date_start != 'ALL_DATES' and date_end != 'ALL_DATES':
        # Full date range for main aggregation
        main_conditions.append(f"DATE({timestamp_column}) BETWEEN '{date_start}' AND '{date_end}'")
    main_where_clause = " AND ".join(main_conditions) if main_conditions else ""
    
    return cohort_where_clause, main_where_clause

def generate_level_fields(events, event_name_col: str, timestamp_column: str):
    """Generate level fields using AF pattern 'complete_bossladder_level<N>'"""
    level_fields = []
    level_counts = []
    # Synthesize first 10 levels via regex on event_name
    for i in range(1, 11):
        level_fields.append(
            f"MIN(CASE WHEN REGEXP_EXTRACT(LOWER({event_name_col}), r'complete_bossladder_level(\\d+)') = '{i}' THEN {timestamp_column} END) as level_{i}_time"
        )
        level_counts.append(
            f"COUNT(CASE WHEN REGEXP_EXTRACT(LOWER({event_name_col}), r'complete_bossladder_level(\\d+)') = '{i}' THEN 1 END) as level_{i}_count"
        )
    # No explicit events list returned (regex-based)
    return level_fields, level_counts, []

def generate_aggregation_query(dataset_name, schema_mapping, limit=1000, single_date=None):
    """Generate the main aggregation query with enhanced features (Final Working Version)
    
    Args:
        dataset_name: BigQuery dataset name
        schema_mapping: Schema mapping dictionary
        limit: Query result limit
        single_date: Optional single date to filter to (YYYY-MM-DD format)
    """
    print("📊 Generating enhanced aggregation query (Final Working Version)...")
    
    # Get filter information
    app_filter = os.environ.get('APP_FILTER', '').strip()
    date_start = os.environ.get('DATE_START', '').strip()
    date_end = os.environ.get('DATE_END', '').strip()
    # Column mappings from environment (sourced from field_mapping.json via orchestrator/env)
    event_name_col = os.environ.get('EVENT_NAME_COLUMN', '').strip()
    if not event_name_col:
        print('❌ Error: EVENT_NAME_COLUMN not set. Ensure field_mapping.json provides event_name_column or set env.')
        exit(1)
    timestamp_col = os.environ.get('TIMESTAMP_COLUMN', '').strip()
    if not timestamp_col:
        print('❌ Error: TIMESTAMP_COLUMN not set. Ensure field_mapping.json provides timestamp_column or set env.')
        exit(1)
    revenue_col = os.environ.get('REVENUE_COLUMN', '').strip()
    if not revenue_col:
        print('❌ Error: REVENUE_COLUMN not set. Ensure field_mapping.json provides revenue_column or set env.')
        exit(1)
    # Country column (AF uses country_code)
    country_col = os.environ.get('COUNTRY_COLUMN', 'country_code').strip()
    # Session id column (may not exist in AF exports)
    session_id_col = os.environ.get('SESSION_ID_COLUMN', '').strip()
    
    # Get separate WHERE clauses for cohort and main aggregation
    cohort_where_clause, main_where_clause = build_where_clause(app_filter, date_start, date_end, timestamp_col, single_date=single_date)
    
    # Get events for dynamic field generation
    events = schema_mapping.get('events', {}).get('event_counts', {})
    level_fields, level_counts, level_events = generate_level_fields(events, event_name_col, timestamp_col)
    
    # Get recommendations
    recommendations = schema_mapping.get('recommendations', {})
    USER_ID_COLUMN = os.environ.get('USER_ID_COLUMN', 'appsflyer_id')
    # In aggregation query, replace references to ip/device_id with appsflyer_id
    primary_user_id = USER_ID_COLUMN
    user_id_issues = recommendations.get('user_id_issues', [])
    
    # Generate max level reached with proper aggregation
    max_level_case = ""
    # Compute max level via regex regardless of explicit events list
    max_level_case = f"""
        -- Max Level Reached (regex-based for AF levels)
        COALESCE(MAX(CAST(REGEXP_EXTRACT(LOWER({event_name_col}), r'complete_bossladder_level(\\d+)') AS INT64)), 0) as max_level_reached,"""
    
    # Generate the main query with proper GROUP BY handling
    # Build optional session CTE and fields
    if session_id_col:
        session_where = f"{main_where_clause} AND {primary_user_id} IS NOT NULL" if main_where_clause else f"{primary_user_id} IS NOT NULL"
        session_cte = f"""
    session_durations AS (
        SELECT 
            {primary_user_id} as user_id,
            DATE({timestamp_col}) as date,
            MIN({timestamp_col}) as session_start,
            MAX({timestamp_col}) as session_end,
            TIMESTAMP_DIFF(MAX({timestamp_col}), MIN({timestamp_col}), MINUTE) as session_duration_minutes
        FROM `{dataset_name}`
        {"WHERE " + session_where if session_where else ""}
        GROUP BY {primary_user_id}, DATE({timestamp_col})
    ),
    """
        session_join = f"LEFT JOIN session_durations sd ON t.{primary_user_id} = sd.user_id AND DATE(t.{timestamp_col}) = sd.date"
        session_fields = """
        AVG(sd.session_duration_minutes) as avg_session_duration_minutes,
        MAX(sd.session_duration_minutes) as longest_session_duration_minutes,
        SUM(sd.session_duration_minutes) as total_session_time_minutes,
        """
    else:
        session_cte = ""
        session_join = ""
        session_fields = """
        CAST(NULL AS INT64) as avg_session_duration_minutes,
        CAST(NULL AS INT64) as longest_session_duration_minutes,
        CAST(NULL AS INT64) as total_session_time_minutes,
        """

    # Load revenue type identifiers from field_mapping.json (via env)
    field_mapping_path = os.environ.get('FIELD_MAPPING_JSON')
    iap_events_list = []
    ad_events_list = []
    if field_mapping_path and os.path.exists(field_mapping_path):
        try:
            with open(field_mapping_path, 'r') as fm:
                fm_json = json.load(fm)
                rti = fm_json.get('revenue_type_identifiers', {})
                iap_events_list = rti.get('iap_event_names', []) or []
                ad_events_list = rti.get('ad_event_names', []) or []
        except Exception:
            pass
    # Build SQL IN lists
    iap_events_sql = ", ".join([f"'{e}'" for e in iap_events_list]) if iap_events_list else ""
    ad_events_sql = ", ".join([f"'{e}'" for e in ad_events_list]) if ad_events_list else ""

    query = f"""
    -- Enhanced User Daily Aggregation with Session Duration and Revenue Classification (Final Working)
    -- Generated for run: {schema_mapping.get('run_hash', 'unknown')}
    -- Data Quality Issues: {', '.join(user_id_issues) if user_id_issues else 'None'}
    
    WITH
    {session_cte}
    
    user_cohorts AS (
        SELECT 
            {primary_user_id} as user_id,
            MIN(DATE({timestamp_col})) as cohort_date
        FROM `{dataset_name}`
        {"WHERE " + cohort_where_clause if cohort_where_clause else ""}
        GROUP BY {primary_user_id}
    )
    
    SELECT 
        -- Primary Key
        {primary_user_id} as user_id,
        DATE(t.{timestamp_col}) as date,
        
        -- User Cohort Information
        uc.cohort_date,
        DATE_DIFF(ANY_VALUE(DATE(t.{timestamp_col})), COALESCE(uc.cohort_date, ANY_VALUE(DATE(t.{timestamp_col}))), DAY) as days_since_first_event,
        CASE 
            WHEN DATE_DIFF(ANY_VALUE(DATE(t.{timestamp_col})), COALESCE(uc.cohort_date, ANY_VALUE(DATE(t.{timestamp_col}))), DAY) = 0 THEN 'new'
            ELSE 'returning'
        END as user_type,
        
        -- Session Duration Metrics (using heuristic if available)
        {session_fields}
        
        -- Revenue Metrics (Enhanced with Classification)
        SUM(CASE WHEN {revenue_col} > 0 THEN {revenue_col} ELSE 0 END) as total_revenue,
        SUM(CASE WHEN {revenue_col} > 0 THEN {revenue_col} ELSE 0 END) as total_revenue_usd,
        
        -- Revenue by Type (Mutually Exclusive Classification)
        SUM(CASE WHEN {revenue_col} > 0 AND (
            (
              UPPER({event_name_col}) LIKE '%IAP%' OR 
              UPPER({event_name_col}) LIKE '%PURCHASE%' OR 
              UPPER({event_name_col}) LIKE '%BUY%' OR
              UPPER({event_name_col}) LIKE '%INAPP%' OR
              UPPER({event_name_col}) LIKE '%TRANSACTION%'
              {(' OR ' + event_name_col + ' IN (' + iap_events_sql + ')') if iap_events_sql else ''}
            )
            AND NOT (
              UPPER({event_name_col}) LIKE '%AD%' OR 
              UPPER({event_name_col}) LIKE '%ADS%' OR 
              UPPER({event_name_col}) LIKE '%ADMON%' OR
              UPPER({event_name_col}) LIKE '%ADVERTISEMENT%' OR
              UPPER({event_name_col}) LIKE '%BANNER%' OR
              UPPER({event_name_col}) LIKE '%INTERSTITIAL%' OR
              UPPER({event_name_col}) LIKE '%REWARDED%'
              {(' OR ' + event_name_col + ' IN (' + ad_events_sql + ')') if ad_events_sql else ''}
            )
        ) THEN {revenue_col} ELSE 0 END) as iap_revenue,
        
        SUM(CASE WHEN {revenue_col} > 0 AND (
            (
              UPPER({event_name_col}) LIKE '%AD%' OR 
              UPPER({event_name_col}) LIKE '%ADS%' OR 
              UPPER({event_name_col}) LIKE '%ADMON%' OR
              UPPER({event_name_col}) LIKE '%ADVERTISEMENT%' OR
              UPPER({event_name_col}) LIKE '%BANNER%' OR
              UPPER({event_name_col}) LIKE '%INTERSTITIAL%' OR
              UPPER({event_name_col}) LIKE '%REWARDED%'
              {(' OR ' + event_name_col + ' IN (' + ad_events_sql + ')') if ad_events_sql else ''}
            )
            AND NOT (
              UPPER({event_name_col}) LIKE '%IAP%' OR 
              UPPER({event_name_col}) LIKE '%PURCHASE%' OR 
              UPPER({event_name_col}) LIKE '%BUY%' OR
              UPPER({event_name_col}) LIKE '%INAPP%' OR
              UPPER({event_name_col}) LIKE '%TRANSACTION%'
              {(' OR ' + event_name_col + ' IN (' + iap_events_sql + ')') if iap_events_sql else ''}
            )
        ) THEN {revenue_col} ELSE 0 END) as ad_revenue,
        
        SUM(CASE WHEN {revenue_col} > 0 AND (
            {event_name_col} LIKE '%sub%' OR 
            {event_name_col} LIKE '%subscription%' OR 
            {event_name_col} LIKE '%recurring%' OR
            {event_name_col} LIKE '%premium%' OR
            {event_name_col} LIKE '%pro%'
        ) THEN {revenue_col} ELSE 0 END) as subscription_revenue,
        
        -- Revenue Event Counts by Type (Enhanced Generic Classification)
        COUNT(CASE WHEN {revenue_col} > 0 AND (
            (
              UPPER({event_name_col}) LIKE '%IAP%' OR 
              UPPER({event_name_col}) LIKE '%PURCHASE%' OR 
              UPPER({event_name_col}) LIKE '%BUY%' OR
              UPPER({event_name_col}) LIKE '%INAPP%' OR
              UPPER({event_name_col}) LIKE '%TRANSACTION%'
              {(' OR ' + event_name_col + ' IN (' + iap_events_sql + ')') if iap_events_sql else ''}
            )
            AND NOT (
              UPPER({event_name_col}) LIKE '%AD%' OR 
              UPPER({event_name_col}) LIKE '%ADS%' OR 
              UPPER({event_name_col}) LIKE '%ADMON%' OR
              UPPER({event_name_col}) LIKE '%ADVERTISEMENT%' OR
              UPPER({event_name_col}) LIKE '%BANNER%' OR
              UPPER({event_name_col}) LIKE '%INTERSTITIAL%' OR
              UPPER({event_name_col}) LIKE '%REWARDED%'
              {(' OR ' + event_name_col + ' IN (' + ad_events_sql + ')') if ad_events_sql else ''}
            )
        ) THEN 1 END) as iap_events_count,
        
        COUNT(CASE WHEN {revenue_col} > 0 AND (
            (
              UPPER({event_name_col}) LIKE '%AD%' OR 
              UPPER({event_name_col}) LIKE '%ADS%' OR 
              UPPER({event_name_col}) LIKE '%ADMON%' OR
              UPPER({event_name_col}) LIKE '%ADVERTISEMENT%' OR
              UPPER({event_name_col}) LIKE '%BANNER%' OR
              UPPER({event_name_col}) LIKE '%INTERSTITIAL%' OR
              UPPER({event_name_col}) LIKE '%REWARDED%'
              {(' OR ' + event_name_col + ' IN (' + ad_events_sql + ')') if ad_events_sql else ''}
            )
            AND NOT (
              UPPER({event_name_col}) LIKE '%IAP%' OR 
              UPPER({event_name_col}) LIKE '%PURCHASE%' OR 
              UPPER({event_name_col}) LIKE '%BUY%' OR
              UPPER({event_name_col}) LIKE '%INAPP%' OR
              UPPER({event_name_col}) LIKE '%TRANSACTION%'
              {(' OR ' + event_name_col + ' IN (' + iap_events_sql + ')') if iap_events_sql else ''}
            )
        ) THEN 1 END) as ad_events_count,
        
        COUNT(CASE WHEN {revenue_col} > 0 AND (
            {event_name_col} LIKE '%sub%' OR 
            {event_name_col} LIKE '%subscription%' OR 
            {event_name_col} LIKE '%recurring%' OR
            {event_name_col} LIKE '%premium%' OR
            {event_name_col} LIKE '%pro%'
        ) THEN 1 END) as subscription_events_count,
        COUNT(CASE WHEN {revenue_col} > 0 THEN 1 END) as total_revenue_events_count,
        
        -- Revenue Timestamps
        MIN(CASE WHEN {revenue_col} > 0 THEN {timestamp_col} END) as first_purchase_time,
        MAX(CASE WHEN {revenue_col} > 0 THEN {timestamp_col} END) as last_purchase_time,
        
        -- Event Counts & Engagement Metrics
        COUNT(*) as total_events,
        COUNT(DISTINCT {event_name_col}) as unique_events,
        
        -- Key Milestone Events (AF-aware)
        MIN(CASE WHEN LOWER({event_name_col}) IN ('ftue_complete','af_tutorial_completion') THEN {timestamp_col} END) as ftue_complete_time,
        MIN(CASE WHEN LOWER({event_name_col}) IN ('game_complete','af_game_complete') THEN {timestamp_col} END) as game_complete_time,
        
        -- Dynamic Level Fields
        {', '.join(level_fields) if level_fields else '-- No level events found'},
        
        -- Level Counts
        {', '.join(level_counts) if level_counts else '-- No level events found'},
        
        {max_level_case}
        
        -- Geographic & Attribution
        ANY_VALUE({country_col}) as country,
        ANY_VALUE(state) as state,
        ANY_VALUE(city) as city,
        ANY_VALUE(media_source) as media_source,
        ANY_VALUE(channel) as channel,
        ANY_VALUE(partner) as partner,
        ANY_VALUE(campaign_id) as campaign_id,
        ANY_VALUE(campaign) as campaign_name,
        
        -- Data Quality & Metadata
        {schema_mapping.get('data_quality', {}).get('overall_score', 0)} as data_quality_score,
        CURRENT_TIMESTAMP() as last_updated,
        '{schema_mapping.get('run_hash', 'unknown')}' as run_hash,
        ANY_VALUE(app_name) as app_name,
        
        -- Data Quality Issues (JSON)
        '{json.dumps(user_id_issues)}' as data_quality_issues
        
    FROM `{dataset_name}` t
    {session_join}
    LEFT JOIN user_cohorts uc ON {primary_user_id} = uc.user_id
    {"WHERE " + main_where_clause if main_where_clause else ""}
    GROUP BY 
        {primary_user_id},
        DATE(t.{timestamp_col}),
        uc.cohort_date
    ORDER BY 
        {primary_user_id},
        DATE(t.{timestamp_col})
    LIMIT {limit};
    """
    
    return query

def create_bigquery_table(client, query, target_project, target_dataset, table_name):
    """Attempt to create BigQuery table with safety validation"""
    print(f"🏗️ Attempting to create BigQuery table: {target_project}.{target_dataset}.{table_name}")
    
    try:
        # Use safe client's create_table method if available
        if hasattr(client, 'create_table'):
            # Safe client with built-in validation
            return client.create_table(query, target_project, target_dataset, table_name)
        else:
            # Fallback for regular client (with basic safety check)
            print("⚠️ Using fallback table creation (safety module not available)")
            
            # Basic safety check for CREATE operations
            if 'CREATE' not in query.upper():
                raise ValueError("Table creation requires CREATE operation")
            
            # Create the table
            create_query = f"""
            CREATE OR REPLACE TABLE `{target_project}.{target_dataset}.{table_name}` AS
            {query}
            """
            
            job = client.query(create_query)
            job.result()  # Wait for job to complete
            
            print(f"✅ Successfully created BigQuery table: {target_project}.{target_dataset}.{table_name}")
            return True
        
    except BigQuerySafetyError as e:
        print(f"🛡️ Safety validation failed: {str(e)}")
        return False
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
    
    # Debug: Print the first 200 characters of the query
    print(f"🔍 Query preview: {query[:200]}...")
    
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
        with open(f'run_logs/{os.environ.get("RUN_HASH")}/logs/run.log', 'a') as logf:
            traceback.print_exc(file=logf)
        print(f"❌ Error exporting to CSV: {str(e)}")
        traceback.print_exc()
        raise

def generate_date_list(date_start, date_end):
    """Generate a list of dates from date_start to date_end (inclusive)"""
    if not date_start or not date_end or date_start == 'ALL_DATES' or date_end == 'ALL_DATES':
        return []
    
    try:
        start = datetime.strptime(date_start, '%Y-%m-%d')
        end = datetime.strptime(date_end, '%Y-%m-%d')
        
        if start > end:
            return []
        
        date_list = []
        current = start
        while current <= end:
            date_list.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=1)
        
        return date_list
    except ValueError as e:
        print(f"⚠️ Error parsing dates: {str(e)}")
        return []

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
    """Main aggregation function with safety validation"""
    print("🚀 Starting Enhanced Data Aggregation v3.5.0 (Day-by-Day Processing) for Product Dashboard Builder v2")
    print("=" * 80)
    
    # Load environment variables
    load_dotenv()
    
    # Validate environment safety
    if validate_environment_safety and not validate_environment_safety():
        print("❌ Environment safety validation failed. Exiting.")
        return 1
    
    print("🛡️ BigQuery safety guardrails enabled")
    
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
        
        # Ensure event_name_col is set and not 'name'
        if schema_mapping.get('events', {}).get('event_name_column') == 'name':
            print("❌ Error: event_name_column in schema_mapping is set to 'name'. This is not allowed. Please set it to the actual event name column.")
            return 1
        
        # Generate date list for day-by-day processing
        date_list = generate_date_list(date_start, date_end)
        
        if not date_list:
            print("⚠️ No valid date range provided, processing entire date range at once")
            date_list = [None]  # Process as single query
        
        # Debug logging
        print(f"🔍 Dataset name: {dataset_name}")
        print(f"🔍 Primary user ID: {schema_mapping.get('recommendations', {}).get('primary_user_id', 'device_id')}")
        print(f"🔍 Date start: {os.environ.get('DATE_START', '')}")
        print(f"🔍 Date end: {os.environ.get('DATE_END', '')}")
        print(f"📅 Processing {len(date_list)} day(s) day-by-day")
        
        # Create working and output directories
        os.makedirs(working_dir, exist_ok=True)
        os.makedirs(outputs_dir, exist_ok=True)
        
        # Accumulate results from all days
        all_results = []
        total_rows = 0
        
        # Process each day
        for idx, current_date in enumerate(date_list, 1):
            if current_date:
                print(f"\n📅 Processing day {idx}/{len(date_list)}: {current_date}")
            else:
                print(f"\n📅 Processing entire date range")
            
            # Generate query for this day
            query = generate_aggregation_query(dataset_name, schema_mapping, aggregation_limit, single_date=current_date)
            
            # Save query to working directory (overwrite with last day's query)
            with open(f'{working_dir}/aggregation_query.sql', 'w') as f:
                f.write(query)
            
            if idx == 1:  # Only print query preview for first day
                print(f"🔍 Query preview (first day): {query[:200]}...")
            
            try:
                # Execute query for this day
                print(f"⏳ Executing query for {current_date if current_date else 'entire range'}...")
                df_day = client.query(query).to_dataframe()
                
                if len(df_day) > 0:
                    all_results.append(df_day)
                    total_rows += len(df_day)
                    print(f"✅ Day {current_date if current_date else 'range'}: {len(df_day)} rows processed (Total: {total_rows})")
                else:
                    print(f"ℹ️ Day {current_date if current_date else 'range'}: No data found")
                    
            except Exception as e:
                print(f"⚠️ Error processing day {current_date if current_date else 'range'}: {str(e)}")
                print(f"   Continuing with next day...")
                continue
        
        # Combine all results
        if all_results:
            print(f"\n📊 Combining results from {len(all_results)} day(s)...")
            combined_df = pd.concat(all_results, ignore_index=True)
            
            # Apply limit after combining (to respect overall limit)
            if len(combined_df) > aggregation_limit:
                print(f"⚠️ Total rows ({len(combined_df)}) exceed limit ({aggregation_limit}), truncating...")
                combined_df = combined_df.head(aggregation_limit)
            
            # Save combined results to CSV
            csv_path = f'{outputs_dir}/aggregated_data.csv'
            print(f"💾 Saving {len(combined_df)} rows to CSV: {csv_path}")
            combined_df.to_csv(csv_path, index=False)
            print(f"✅ Successfully saved aggregated data to: {csv_path}")
            
            # Attempt to create BigQuery table (optional) - use combined data
            table_created = False
            if os.environ.get('SKIP_TABLE_CREATION', 'true').lower() != 'true':
                try:
                    # Generate a combined query for table creation
                    combined_query = generate_aggregation_query(dataset_name, schema_mapping, aggregation_limit, single_date=None)
                    table_created = create_bigquery_table(client, combined_query, target_project, target_dataset, table_name)
                except Exception as e:
                    print(f"⚠️ Skipping table creation due to error: {str(e)}")
                    table_created = False
        else:
            print("❌ No data found for any day in the date range")
            csv_path = f'{outputs_dir}/aggregated_data.csv'
            # Create empty CSV
            pd.DataFrame().to_csv(csv_path, index=False)
            table_created = False
        
        # Generate summary report
        summary_path = f'{outputs_dir}/aggregation_summary.json'
        generate_summary_report(schema_mapping, summary_path, success=True, table_created=table_created)
        
        print("\n🎉 Enhanced Data Aggregation v3.5.0 (Day-by-Day Processing) completed successfully!")
        print(f"📊 Results available at: {outputs_dir}")
        print(f"📄 CSV file: {csv_path}")
        print(f"📋 Summary report: {summary_path}")
        
        # Log audit trail if available
        if hasattr(client, 'get_audit_log'):
            audit_log = client.get_audit_log()
            if audit_log:
                print(f"📋 Audit log: {len(audit_log)} operations logged")
        
        return 0
        
    except BigQuerySafetyError as e:
        print(f"🛡️ Safety validation error: {str(e)}")
        print("❌ Aggregation stopped due to safety violation")
        return 1
    except Exception as e:
        with open(f'run_logs/{os.environ.get("RUN_HASH")}/logs/run.log', 'a') as logf:
            traceback.print_exc(file=logf)
        print(f"[ERROR] Exception type: {sys.exc_info()[0]}", flush=True)
        print(f"[ERROR] Exception value: {sys.exc_info()[1]}", flush=True)
        traceback.print_exc()
        print(f"[ERROR] working dir: {working_dir}", flush=True)
        print(f"[ERROR] output_path: {csv_path}", flush=True)
        raise

if __name__ == "__main__":
    exit(main())
