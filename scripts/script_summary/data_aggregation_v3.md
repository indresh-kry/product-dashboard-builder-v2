# Data Aggregation v3 Script Documentation

## Script Overview

**File**: `scripts/data_aggregation_v3.py`  
**Version**: 3.1.0  
**Purpose**: Final working version of data aggregation with session duration calculation, enhanced revenue classification, and comprehensive user-daily aggregation.

## Functions

### Core Connection Functions

#### `get_bigquery_client()`
- **Purpose**: Initialize BigQuery client with credentials
- **Parameters**: None
- **Returns**: BigQuery client instance
- **Description**: Creates authenticated BigQuery client using service account credentials

#### `load_schema_mapping(run_hash)`
- **Purpose**: Load schema mapping from previous run
- **Parameters**:
  - `run_hash`: Unique run identifier
- **Returns**: Schema mapping dictionary or None on error
- **Description**: Loads schema mapping JSON file created by schema discovery phase

#### `build_where_clause(app_filter, date_start, date_end)`
- **Purpose**: Build WHERE clause for SQL queries based on filters
- **Parameters**:
  - `app_filter`: App name filter (optional)
  - `date_start`: Start date filter (optional)
  - `date_end`: End date filter (optional)
- **Returns**: SQL WHERE clause string
- **Description**: Constructs WHERE clause for filtering data by app and date range

### Dynamic Field Generation Functions

#### `generate_level_fields(events)`
- **Purpose**: Generate dynamic level fields based on available events
- **Parameters**:
  - `events`: Dictionary of event counts from schema mapping
- **Returns**: Tuple of (level_fields, level_counts, level_events)
- **Description**: Dynamically generates SQL fields for level events based on discovered events

### Query Generation Functions

#### `generate_aggregation_query(dataset_name, schema_mapping, limit=1000)`
- **Purpose**: Generate the main aggregation query with enhanced features
- **Parameters**:
  - `dataset_name`: BigQuery dataset name
  - `schema_mapping`: Schema mapping from discovery phase
  - `limit`: Maximum number of rows to return
- **Returns**: SQL query string
- **Description**: Generates comprehensive user-daily aggregation query with:
  - Session duration calculation using CTEs
  - User cohort definition
  - Revenue classification (IAP, ads, subscription)
  - Dynamic level fields
  - Data quality metrics
  - Geographic and attribution data

### Data Export Functions

#### `create_bigquery_table(client, query, target_project, target_dataset, table_name)`
- **Purpose**: Attempt to create BigQuery table with aggregation results
- **Parameters**:
  - `client`: BigQuery client instance
  - `query`: SQL query to execute
  - `target_project`: Target project ID
  - `target_dataset`: Target dataset name
  - `table_name`: Target table name
- **Returns**: Boolean indicating success
- **Description**: Attempts to create BigQuery table, handles permission errors gracefully

#### `export_to_csv(client, query, output_path)`
- **Purpose**: Export aggregation results to CSV file
- **Parameters**:
  - `client`: BigQuery client instance
  - `query`: SQL query to execute
  - `output_path`: Path for CSV output file
- **Returns**: Boolean indicating success
- **Description**: Executes query and exports results to CSV with proper formatting

### Reporting Functions

#### `generate_summary_report(schema_mapping, output_path, success=True, table_created=False)`
- **Purpose**: Generate summary report for aggregation run
- **Parameters**:
  - `schema_mapping`: Schema mapping from discovery phase
  - `output_path`: Path for report output
  - `success`: Whether aggregation was successful
  - `table_created`: Whether BigQuery table was created
- **Returns**: None
- **Description**: Creates comprehensive summary report with run details and data quality metrics

#### `main()`
- **Purpose**: Main entry point for data aggregation
- **Parameters**: None
- **Returns**: None
- **Description**: Orchestrates the complete data aggregation process

## Tools & External Dependencies

### Google Cloud Tools
- **google-cloud-bigquery**: BigQuery client for data aggregation, table creation, and query execution
- **google-oauth2**: Service account authentication and credential management for Google Cloud services

### Data Processing Tools
- **pandas**: Data manipulation, analysis, and CSV export operations
- **python-dotenv**: Environment variable loading and configuration management

### File System Tools
- **pathlib.Path**: Cross-platform path handling and directory operations
- **json**: JSON serialization, deserialization, and configuration file I/O
- **os**: Environment variables, system operations, and file permissions

### Data Analysis Tools
- **datetime**: Date and time operations, formatting, and calculations
- **typing**: Type hints and annotations for better code documentation

## Variables & Configuration

### Input Variables (Function Parameters)
- **`run_hash`**: String identifier for the current analysis run
- **`dataset_name`**: BigQuery dataset name for data source
- **`schema_mapping`**: Dictionary containing schema mapping from discovery phase
- **`limit`**: Integer limit for aggregation query results (default: 1000)
- **`app_filter`**: String for filtering by app name (optional)
- **`date_start`**: String for analysis start date (YYYY-MM-DD format, optional)
- **`date_end`**: String for analysis end date (YYYY-MM-DD format, optional)

### Environment Variables (from .env files)
- **`AGGREGATION_LIMIT`**: Integer limit for aggregation data (default: 1000)
- **`GOOGLE_CLOUD_PROJECT`**: Google Cloud project ID for BigQuery access
- **`GOOGLE_APPLICATION_CREDENTIALS`**: Path to Google Cloud service account credentials
- **`DATASET_NAME`**: BigQuery dataset name for data source
- **`APP_FILTER`**: String for filtering by app name (optional)
- **`DATE_START`**: String for analysis start date (YYYY-MM-DD format, optional)
- **`DATE_END`**: String for analysis end date (YYYY-MM-DD format, optional)
- **`RUN_HASH`**: Unique identifier for the current run

### Hardcoded Variables
- **`REVENUE_CLASSIFICATION_PATTERNS`**: Dictionary defining revenue type classification patterns
  - `iap_patterns`: ['%iap%', '%purchase%', '%buy%', '%transaction%']
  - `ad_patterns`: ['%ad%', '%ads%', '%admon%', '%reward%']
  - `subscription_patterns`: ['%sub%', '%subscription%', '%recurring%']
- **`SESSION_DURATION_CALCULATION`**: SQL logic for calculating session duration from timestamps
- **`COHORT_ANALYSIS_WINDOWS`**: List of cohort analysis windows [0, 1, 3, 7, 14, 30, 60]
- **`DEFAULT_AGGREGATION_FIELDS`**: List of standard aggregation fields for user-daily data
- **`OUTPUT_FILE_NAMES`**: Dictionary mapping output types to filenames
  - `aggregated_data`: 'aggregated_data.csv'
  - `summary_report`: 'aggregation_summary.json'
  - `sql_query`: 'aggregation_query.sql'

### Computed Variables
- **`primary_user_id`**: User identifier field determined from schema mapping (custom_user_id or device_id)
- **`level_fields`**: Dynamic SQL fields for level events based on discovered events
- **`level_counts`**: Dynamic SQL fields for level event counts
- **`level_events`**: List of discovered level events for dynamic field generation
- **`where_clause`**: SQL WHERE clause constructed from app and date filters
- **`extended_date_start`**: Extended start date (7 days prior) for cohort assignment
- **`aggregation_query`**: Complete SQL query for user-daily aggregation with CTEs
- **`session_durations_cte`**: Common Table Expression for session duration calculations
- **`user_cohorts_cte`**: Common Table Expression for user cohort assignment
- **`aggregation_summary`**: Dictionary containing aggregation metadata and statistics
- **`output_paths`**: Dictionary mapping output types to file paths

## Flow Diagram

```mermaid
graph TD
    A[main] --> B[Load Environment Variables]
    B --> C[get_bigquery_client]
    C --> D[load_schema_mapping]
    
    D --> E[generate_aggregation_query]
    E --> F[build_where_clause]
    F --> G[generate_level_fields]
    G --> H[Generate SQL Query with CTEs]
    
    H --> I[create_bigquery_table]
    I --> J{Table Creation Successful?}
    
    J -->|Yes| K[Report Table Created]
    J -->|No| L[export_to_csv]
    
    L --> M[Execute Query]
    M --> N[Export to CSV]
    
    K --> O[generate_summary_report]
    N --> O
    
    O --> P[Save Summary Report]
    P --> Q[Save SQL Query]
    Q --> R[Complete]
    
    style A fill:#e1f5fe
    style C fill:#e8f5e8
    style D fill:#e8f5e8
    style E fill:#e8f5e8
    style I fill:#fff3e0
    style L fill:#fff3e0
    style O fill:#e8f5e8
    style R fill:#c8e6c9
```

## Usage Examples

### Direct Execution
```bash
# Run with current environment
python scripts/data_aggregation_v3.py

# Run with specific configuration
AGGREGATION_LIMIT=1000 python scripts/data_aggregation_v3.py
```

### From Orchestrator
```bash
# Data aggregation is automatically run in Phase 2
python scripts/analysis_workflow_orchestrator.py
```

### Environment Setup
```bash
# Set required environment variables
export RUN_HASH=abc123
export DATASET_NAME=gc-prod-459709.nbs_dataset.singular_user_level_event_data
export APP_FILTER=com.nukebox.mandir
export DATE_START=2025-09-01
export DATE_END=2025-09-07
export AGGREGATION_LIMIT=1000
export GOOGLE_CLOUD_PROJECT=gc-prod-459709
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json

# Run data aggregation
python scripts/data_aggregation_v3.py
```

## Dependencies

### Required Packages
- **google-cloud-bigquery**: BigQuery client library
- **google-oauth2**: OAuth2 authentication for Google Cloud
- **pandas**: Data manipulation and analysis
- **python-dotenv**: Environment variable loading
- **json**: JSON serialization (built-in)
- **datetime**: Date/time operations (built-in)
- **pathlib**: Path handling (built-in)
- **os**: Environment variable access (built-in)

### Environment Variables
- **RUN_HASH**: Unique identifier for the current run
- **DATASET_NAME**: BigQuery dataset name
- **APP_FILTER**: Filter by app name (optional)
- **DATE_START**: Start date for filtering (optional)
- **DATE_END**: End date for filtering (optional)
- **AGGREGATION_LIMIT**: Maximum rows to return (default: 1000)
- **GOOGLE_CLOUD_PROJECT**: Google Cloud project ID
- **GOOGLE_APPLICATION_CREDENTIALS**: Path to service account credentials

### Optional Environment Variables
- **TARGET_PROJECT**: Target project for table creation
- **TARGET_DATASET**: Target dataset for table creation
- **AGGREGATION_TABLE_NAME**: Name for aggregation table

## Output Files

### Aggregation Outputs
- **user_daily_aggregation_v3.csv**: Aggregated user-daily data
- **aggregation_sql_v3.sql**: Generated SQL query
- **aggregation_summary_report_v3.json**: Summary report with run details

### Working Files
- **aggregation_sql_enhanced.sql**: SQL query saved to working directory

## Aggregation Features

### Session Duration Calculation
- Uses CTEs to calculate session durations from timestamps
- Calculates average, maximum, and total session time
- Handles multiple sessions per day per user

### User Cohort Definition
- Identifies user cohorts based on first event date
- Calculates days since first event
- Classifies users as new or returning

### Enhanced Revenue Classification
- **Generic Pattern Matching**: Uses robust pattern matching for revenue type detection
- **IAP Detection**: Identifies iap, purchase, buy, inapp, transaction events
- **AdMon Detection**: Identifies ad, ads, admon, advertisement, banner, interstitial, rewarded events
- **Subscription Detection**: Identifies sub, subscription, recurring, premium, pro events
- **Primary Filter**: Uses `is_revenue_valid = true` as first-level check
- **Column Source**: Uses `name` column instead of `received_revenue_event` for accurate classification
- Calculates revenue totals by type
- Counts revenue events by type
- Tracks first and last purchase times

### Dynamic Level Fields
- Generates level fields based on discovered events
- Creates level completion timestamps
- Calculates level completion counts
- Determines maximum level reached

### Data Quality Metrics
- Includes data quality score from schema discovery
- Tracks run hash for data lineage
- Records last updated timestamp
- Identifies data quality issues

### Geographic and Attribution
- Captures country, state, city information
- Records acquisition channel and campaign data
- Tracks UTM parameters
- Maintains app name for multi-app datasets

## SQL Query Structure

### Common Table Expressions (CTEs)
1. **session_durations**: Calculates session duration metrics
2. **user_cohorts**: Defines user cohorts based on first event date

### Main Query Features
- User-daily aggregation with comprehensive metrics
- Session duration calculations
- Revenue classification and totals
- Dynamic level field generation
- Data quality and metadata tracking
- Geographic and attribution data

## Error Handling

### Connection Errors
- BigQuery authentication failures
- Network connectivity issues
- Dataset access permissions

### Query Errors
- SQL syntax errors
- Column reference errors
- Data type mismatches
- Resource limits exceeded

### Export Errors
- File system permissions
- CSV export failures
- JSON serialization issues

### Fallback Mechanisms
- BigQuery table creation failure → CSV export
- Permission denied → Graceful degradation
- Query timeout → Error reporting

## Integration Points

### With Schema Discovery
- Loads schema mapping for dynamic field generation
- Uses event analysis for level field creation
- Applies data quality metrics from discovery

### With Orchestrator
- Called automatically in Phase 2 of the analysis workflow
- Provides aggregated data for subsequent phases
- Reports success/failure status

### With Other Scripts
- Provides aggregated data for user segmentation
- Supplies metric tables for LLM insights
- Creates foundation for quality assurance validation
