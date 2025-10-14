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
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

try:
    from scripts import rules_engine_integration
except ImportError:  # pragma: no cover - optional dependency during bootstrap
    rules_engine_integration = None

def get_bigquery_client():
    """Initialize BigQuery client with credentials and safe fallbacks."""
    credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    dataset_name = os.environ.get("DATASET_NAME")
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

    if not credentials_path:
        raise EnvironmentError(
            "GOOGLE_APPLICATION_CREDENTIALS not set. "
            "Run Phase 0 bootstrap or supply credentials path."
        )

    if not os.path.exists(credentials_path):
        raise FileNotFoundError(
            f"Credentials file '{credentials_path}' not found. "
            "Check your Phase 0 setup."
        )

    if not project_id:
        inferred = infer_project_from_dataset(dataset_name)
        if inferred:
            project_id = inferred
            os.environ.setdefault("GOOGLE_CLOUD_PROJECT", inferred)
        else:
            raise EnvironmentError(
                "GOOGLE_CLOUD_PROJECT not set and could not infer from DATASET_NAME."
            )

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
        safe_app = sanitize_filter_value(app_filter, "APP_FILTER")
        conditions.append(f"app_longname = '{safe_app}'")
        print(f"🎯 App Filter: {safe_app}")
    else:
        print("🎯 App Filter: None (analyzing all apps)")
    
    safe_start = validate_date(date_start, "DATE_START") if date_start else ""
    safe_end = validate_date(date_end, "DATE_END") if date_end else ""

    if safe_start and safe_end:
        if safe_start > safe_end:
            raise ValueError("DATE_START cannot be later than DATE_END.")
        conditions.append(
            f"DATE(adjusted_timestamp) BETWEEN '{safe_start}' AND '{safe_end}'"
        )
        print(f"📅 Date Range: {safe_start} to {safe_end}")
    elif safe_start:
        conditions.append(f"DATE(adjusted_timestamp) >= '{safe_start}'")
        print(f"📅 Date Range: from {safe_start}")
    elif safe_end:
        conditions.append(f"DATE(adjusted_timestamp) <= '{safe_end}'")
        print(f"📅 Date Range: until {safe_end}")
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

def create_schema_mapping(
    schema_info: List[Dict[str, object]],
    user_candidates: List[Dict[str, object]],
    revenue_columns: List[Dict[str, object]],
    event_taxonomy_records: List[Dict[str, object]],
    quality_metrics: Dict[str, Dict[str, object]],
    available_apps_df: Optional[pd.DataFrame],
    date_range_df: Optional[pd.DataFrame],
    filters: Dict[str, str],
    user_rules: Dict[str, object],
    iap_schema: Dict[str, object],
    admon_schema: Dict[str, object],
) -> Dict[str, object]:
    """Create the master schema mapping with enriched metadata."""
    print("📝 Creating master schema mapping...")

    best_user_column = (
        user_candidates[0]["column_name"] if user_candidates else user_rules["hierarchy"][0]["field"]
    )

    if quality_metrics:
        avg_null = sum(q["null_percentage"] for q in quality_metrics.values()) / len(quality_metrics)
        data_quality_score = round(max(0, 100 - avg_null), 2)
    else:
        data_quality_score = 0.0

    available_apps_data: List[Dict[str, object]] = []
    if available_apps_df is not None and len(available_apps_df) > 0:
        available_apps_data = available_apps_df.to_dict("records")

    date_range_data: Dict[str, object] = {}
    if date_range_df is not None and len(date_range_df) > 0:
        date_range_data = date_range_df.to_dict("records")[0]

    category_breakdown: Dict[str, int] = {}
    for record in event_taxonomy_records:
        category = record.get("category", "uncategorized")
        category_breakdown[category] = category_breakdown.get(category, 0) + 1

    schema_mapping = {
        "version": "1.0",
        "run_hash": os.environ.get("RUN_HASH"),
        "generated_at": datetime.now().isoformat(),
        "table": os.environ.get("DATASET_NAME"),
        "filters": {
            "app_filter": filters.get("app_filter") or "ALL_APPS",
            "date_start": filters.get("date_start") or "ALL_DATES",
            "date_end": filters.get("date_end") or "ALL_DATES",
        },
        "data_quality_score": data_quality_score,
        "user_identification": {
            "primary_field": best_user_column,
            "candidates": user_candidates,
            "rules": user_rules,
        },
        "revenue_calculation": {
            "columns_analyzed": revenue_columns,
            "iap": iap_schema,
            "admon": admon_schema,
        },
        "event_taxonomy": {
            "total_events": len(event_taxonomy_records),
            "category_breakdown": category_breakdown,
            "top_events": [rec["event_name"] for rec in event_taxonomy_records[:10]],
        },
        "data_quality": quality_metrics,
        "schema_info": schema_info,
        "available_apps": available_apps_data,
        "available_date_range": date_range_data,
    }

    print("✅ Master schema mapping created")
    return schema_mapping

def main() -> int:
    """Main schema discovery function."""
    print("🚀 Starting Enhanced Schema Discovery for Product Dashboard Builder v2")
    print("=" * 80)

    run_hash = os.environ.get("RUN_HASH")
    dataset_name = os.environ.get("DATASET_NAME")

    if not run_hash or not dataset_name:
        print("❌ RUN_HASH and DATASET_NAME must be set before running schema discovery.")
        return 1

    raw_app_filter = os.environ.get("APP_FILTER", "")
    raw_date_start = os.environ.get("DATE_START", "")
    raw_date_end = os.environ.get("DATE_END", "")

    try:
        app_filter = sanitize_filter_value(raw_app_filter, "APP_FILTER") if raw_app_filter else ""
        date_start = validate_date(raw_date_start, "DATE_START") if raw_date_start else ""
        date_end = validate_date(raw_date_end, "DATE_END") if raw_date_end else ""
    except ValueError as exc:
        print(f"❌ {exc}")
        return 1

    filters = {"app_filter": app_filter, "date_start": date_start, "date_end": date_end}

    # Ensure downstream helpers read sanitised values.
    os.environ["APP_FILTER"] = app_filter
    os.environ["DATE_START"] = date_start
    os.environ["DATE_END"] = date_end

    outputs_path = ensure_directory(f"run_logs/{run_hash}/outputs/schema")

    print(f"Run Hash: {run_hash}")
    print(f"Dataset: {dataset_name}")
    print(f"App Filter: {app_filter or 'ALL_APPS'}")
    print(
        "Date Range: "
        f"{date_start or 'ALL_DATES'} to {date_end or 'ALL_DATES'}"
    )
    print(f"Outputs Directory: {outputs_path}")
    print()

    try:
        client = get_bigquery_client()
    except Exception as exc:
        print(f"❌ Unable to initialise BigQuery client: {exc}")
        return 1

    try:
        schema_info = discover_schema(client, dataset_name)
        available_apps_df = get_available_apps(client, dataset_name)
        date_range_df = get_available_date_range(
            client, dataset_name, app_filter if app_filter else None
        )

        sample_df = sample_data(client, dataset_name)
        data_available = sample_df is not None and len(sample_df) > 0
        if not data_available:
            print(
                "⚠️  No sample data found for the provided filters. "
                "Proceeding with structural outputs only."
            )

        event_taxonomy_df = analyze_event_taxonomy(client, dataset_name)
        event_taxonomy_records = annotate_event_taxonomy(event_taxonomy_df)

        user_candidates = identify_user_columns(client, dataset_name, sample_df)
        revenue_columns = analyze_revenue_columns(client, dataset_name, sample_df)
        quality_metrics = assess_data_quality(client, dataset_name, sample_df)

        user_rules = build_user_identification_rules(user_candidates)
        iap_schema, admon_schema = build_revenue_schemas(
            event_taxonomy_records, revenue_columns, quality_metrics
        )

        schema_mapping = create_schema_mapping(
            schema_info,
            user_candidates,
            revenue_columns,
            event_taxonomy_records,
            quality_metrics,
            available_apps_df,
            date_range_df,
            filters,
            user_rules,
            iap_schema,
            admon_schema,
        )

        print("\n💾 Saving outputs...")
        write_json(outputs_path / "column_definitions.json", schema_info)
        write_json(outputs_path / "user_identification_analysis.json", user_candidates)
        write_json(outputs_path / "revenue_analysis.json", revenue_columns)
        write_json(outputs_path / "data_quality_report.json", quality_metrics)
        write_json(outputs_path / "event_taxonomy.json", event_taxonomy_records)
        write_json(outputs_path / "user_identification_rules.json", user_rules)
        write_json(outputs_path / "iap_revenue_schema.json", iap_schema)
        write_json(outputs_path / "admon_revenue_schema.json", admon_schema)
        write_json(outputs_path / "schema_mapping.json", schema_mapping)

        if event_taxonomy_df is not None:
            event_taxonomy_df.to_csv(outputs_path / "event_taxonomy.csv", index=False)
        if available_apps_df is not None:
            available_apps_df.to_csv(outputs_path / "available_apps.csv", index=False)

        write_data_quality_markdown(
            outputs_path / "data_quality_report.md",
            run_hash,
            dataset_name,
            filters,
            schema_mapping["data_quality_score"],
            available_apps_df,
            quality_metrics,
            schema_mapping["event_taxonomy"]["category_breakdown"],
        )

        if rules_engine_integration:
            try:
                validation_payload = rules_engine_integration.validate_schema_mapping(
                    schema_mapping=schema_mapping,
                    output_dir=str(outputs_path),
                    user_rules=user_rules,
                    event_taxonomy=event_taxonomy_records,
                    revenue_schemas={"iap": iap_schema, "admon": admon_schema},
                )
                write_json(outputs_path / "rules_validation.json", validation_payload)
                print("✅ Rules engine integration completed.")
            except Exception as exc:  # pragma: no cover - defensive logging
                print(f"⚠️  Rules engine integration failed: {exc}")
        else:
            print("⚠️  Rules engine integration module not available; skipping validation.")

        if data_available:
            print("✅ All outputs saved successfully!")
            print(f"📁 Outputs saved to: {outputs_path}")
            print(f"📄 Master schema mapping: {outputs_path / 'schema_mapping.json'}")
            return 0

        print(
            "⚠️  Completed with warnings: no sample rows matched the filters. "
            "Review available apps/date range and rerun."
        )
        return 1

    except Exception as exc:  # pragma: no cover - unexpected failures
        print(f"❌ Error during schema discovery: {exc}")
        import traceback

        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
# Allowed characters: letters, numbers, underscore, dash, space, comma and dot
SAFE_FILTER_PATTERN = re.compile(r"^[\w\s\-,.]+$")
EVENT_CATEGORY_KEYWORDS: Dict[str, Tuple[str, ...]] = {
    "monetization": ("iap", "purchase", "billing", "revenue", "payment", "subscription"),
    "progression": ("level", "quest", "tutorial", "mission", "milestone", "checkpoint"),
    "engagement": ("session", "login", "share", "social", "chat", "invite"),
    "technical": ("error", "crash", "latency", "performance", "load", "exception"),
    "attribution": ("install", "campaign", "source", "utm", "acquisition"),
}


def ensure_directory(path: str) -> Path:
    """Create an output directory if it does not already exist."""
    directory = Path(path)
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def write_json(path: Path, payload: object) -> None:
    """Write JSON payload with indentation."""
    path.write_text(json.dumps(payload, indent=2))


def write_data_quality_markdown(
    path: Path,
    run_hash: str,
    dataset_name: str,
    filters: Dict[str, str],
    data_quality_score: float,
    available_apps_df: Optional[pd.DataFrame],
    quality_metrics: Dict[str, Dict[str, object]],
    category_breakdown: Dict[str, int],
) -> None:
    """Render a human-friendly markdown report for quality metrics."""
    lines = [
        f"# Data Quality Report - Run {run_hash}",
        "",
        f"**Dataset**: {dataset_name}",
        f"**App Filter**: {filters.get('app_filter') or 'ALL_APPS'}",
        f"**Date Range Start**: {filters.get('date_start') or 'ALL_DATES'}",
        f"**Date Range End**: {filters.get('date_end') or 'ALL_DATES'}",
        f"**Generated**: {datetime.now().isoformat()}",
        "",
        "## Summary",
        "",
        f"- **Total Columns**: {len(quality_metrics) or 'n/a'}",
        f"- **Data Quality Score**: {data_quality_score}%",
        "",
    ]

    if category_breakdown:
        lines.extend(
            [
                "## Event Category Breakdown",
                "",
                "| Category | Events |",
                "|----------|--------|",
            ]
        )
        for category, count in sorted(category_breakdown.items()):
            lines.append(f"| {category} | {count} |")
        lines.append("")

    if available_apps_df is not None and len(available_apps_df) > 0:
        lines.extend(
            [
                "## Available Apps (Top 10)",
                "",
                "| App Name | Event Count | Earliest Date | Latest Date |",
                "|----------|-------------|---------------|-------------|",
            ]
        )
        for _, row in available_apps_df.head(10).iterrows():
            lines.append(
                f"| {row['app_longname']} | {row['event_count']:,} | "
                f"{row['earliest_date']} | {row['latest_date']} |"
            )
        lines.append("")

    if quality_metrics:
        lines.extend(
            [
                "## Column Quality Metrics",
                "",
                "| Column | Null % | Data Type | Unique Values | Duplicate % |",
                "|--------|--------|-----------|---------------|-------------|",
            ]
        )
        for column, metrics in quality_metrics.items():
            lines.append(
                f"| {column} | {metrics['null_percentage']}% | "
                f"{metrics['data_type']} | {metrics['unique_values']} | "
                f"{metrics['duplicate_percentage']}% |"
            )

    path.write_text("\n".join(lines))


def sanitize_filter_value(value: str, name: str) -> str:
    """Ensure filter values are safe to interpolate into SQL."""
    value = value.strip()
    if not value:
        return value
    if not SAFE_FILTER_PATTERN.fullmatch(value):
        raise ValueError(
            f"Invalid characters detected in {name}. "
            "Allowed characters: letters, numbers, spaces, hyphen, comma, period, underscore."
        )
    return value


def validate_date(date_value: str, name: str) -> str:
    """Validate and normalise YYYY-MM-DD date strings."""
    if not date_value:
        return ""
    try:
        return datetime.strptime(date_value.strip(), "%Y-%m-%d").date().isoformat()
    except ValueError as exc:
        raise ValueError(f"{name} must be in YYYY-MM-DD format") from exc


def categorize_event(event_name: str) -> str:
    """Return an event category based on keyword heuristics."""
    name_lower = (event_name or "").lower()
    for category, keywords in EVENT_CATEGORY_KEYWORDS.items():
        if any(keyword in name_lower for keyword in keywords):
            return category
    return "uncategorized"


def annotate_event_taxonomy(df: Optional[pd.DataFrame]) -> List[Dict[str, object]]:
    """Convert taxonomy dataframe to annotated JSON-friendly records."""
    if df is None or df.empty:
        return []

    records: List[Dict[str, object]] = []
    for record in df.to_dict("records"):
        record["category"] = categorize_event(record.get("event_name"))
        if "first_seen" in record and record["first_seen"]:
            record["first_seen"] = str(record["first_seen"])
        if "last_seen" in record and record["last_seen"]:
            record["last_seen"] = str(record["last_seen"])
        records.append(record)
    return records


def build_revenue_schemas(
    taxonomy_records: Iterable[Dict[str, object]],
    revenue_columns: List[Dict[str, object]],
    quality_metrics: Dict[str, Dict[str, object]],
) -> Tuple[Dict[str, object], Dict[str, object]]:
    """Generate IAP and AdMon schema definitions based on heuristics."""
    taxonomy_records = list(taxonomy_records)

    def _find_events(keywords: Tuple[str, ...]) -> List[str]:
        selected = {
            record["event_name"]
            for record in taxonomy_records
            if any(keyword in record["event_name"].lower() for keyword in keywords)
        }
        return sorted(selected)

    def _select_revenue_column(preferred_keywords: Tuple[str, ...]) -> Optional[str]:
        for column in revenue_columns:
            name_lower = column["column_name"].lower()
            if any(keyword in name_lower for keyword in preferred_keywords):
                return column["column_name"]
        return None

    iap_events = _find_events(("iap", "purchase", "checkout", "payment", "store"))
    ad_events = _find_events(("ad", "reward", "interstitial", "banner", "impression"))

    converted_col = _select_revenue_column(("converted_revenue", "revenue"))
    raw_revenue_col = _select_revenue_column(("ad_revenue", "monetization"))
    validation_col = _select_revenue_column(("is_revenue_event", "is_iap"))

    def _quality_note(column_name: Optional[str]) -> Optional[str]:
        if not column_name:
            return None
        metrics = quality_metrics.get(column_name)
        if not metrics:
            return None
        return (
            f"Null rate {metrics['null_percentage']}%, "
            f"unique values {metrics['unique_values']}."
        )

    iap_schema = {
        "version": "1.0",
        "events": iap_events,
        "revenue_field": converted_col or raw_revenue_col,
        "validation_field": validation_col,
        "filters": "is_revenue_event = TRUE" if validation_col else None,
        "refund_handling": "Exclude negative revenue rows; flag duplicates via transaction_id where available.",
        "aggregation": {
            "dimensions": ["event_date", "country", "platform"],
            "metrics": ["SUM(revenue)", "COUNT(DISTINCT custom_user_id)"],
        },
        "data_quality_notes": _quality_note(converted_col or raw_revenue_col),
    }

    admon_schema = {
        "version": "1.0",
        "events": ad_events,
        "revenue_field": raw_revenue_col or converted_col,
        "validation_field": validation_col,
        "filters": "event_category = 'ad'" if any("ad_" in e for e in ad_events) else None,
        "aggregation": {
            "dimensions": ["event_date", "placement", "country"],
            "metrics": ["SUM(revenue)", "AVG(ecpm)"],
        },
        "data_quality_notes": _quality_note(raw_revenue_col or converted_col),
    }

    return iap_schema, admon_schema


def build_user_identification_rules(
    user_candidates: List[Dict[str, object]]
) -> Dict[str, object]:
    """Convert user candidate analysis into hierarchy rules."""
    if not user_candidates:
        return {
            "hierarchy": [
                {"field": "custom_user_id", "usage": "Primary identifier when available"},
                {"field": "device_id", "usage": "Fallback when user id missing"},
            ],
            "deduplication": {
                "strategy": "Use (device_id, event_date) pairs to track potential duplicates.",
                "confidence": 0.4,
            },
            "device_sharing_guidance": "Monitor for >3 distinct users per device per 7 days.",
        }

    hierarchy = []
    for index, candidate in enumerate(user_candidates):
        null_pct = candidate.get("null_percentage", 100)
        confidence = max(0.1, round(1 - (null_pct / 100), 2))
        hierarchy.append(
            {
                "field": candidate["column_name"],
                "usage": "Primary identifier" if index == 0 else "Fallback identifier",
                "confidence": confidence,
                "null_percentage": null_pct,
            }
        )

    dedupe_source = user_candidates[0]["column_name"]
    rules = {
        "hierarchy": hierarchy,
        "deduplication": {
            "strategy": f"Deduplicate on ({dedupe_source}, device_id, DATE(adjusted_timestamp)).",
            "confidence": hierarchy[0]["confidence"],
        },
        "device_sharing_guidance": "Escalate when >5 devices map to the same identifier within 24h.",
    }
    return rules


def infer_project_from_dataset(dataset_name: Optional[str]) -> Optional[str]:
    """Infer the Google Cloud project id from dataset string."""
    if not dataset_name or "." not in dataset_name:
        return None
    return dataset_name.split(".", 1)[0]
