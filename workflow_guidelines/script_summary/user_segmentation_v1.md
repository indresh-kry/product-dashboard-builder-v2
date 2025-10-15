# User Segmentation v1 Script Documentation

## Overview
The `user_segmentation_v1.py` script implements Phase 3 of the analysis workflow, creating comprehensive user segments based on install cohorts, behavioral patterns, and revenue contribution. This script processes aggregated data from Phase 2 to generate segment classifications and statistical metrics.

## Version Information
- **Version**: 1.0.0
- **Last Updated**: 2025-10-14
- **Status**: Active
- **Phase**: 3 - User Segmentation

## Purpose
Create user segments for product analysis including:
- Install cohorts (day-level)
- Behavioral segments (engagement-based)
- Revenue segments (contribution-based)
- Retention cohort analysis
- User journey progression tracking

## Functions

### `load_aggregated_data(run_hash: str) -> pd.DataFrame`
**Purpose**: Load aggregated data from Phase 2 output
**Parameters**:
- `run_hash`: Unique identifier for the current run
**Returns**: DataFrame with aggregated user-daily data
**External Tools**: 
- Google Cloud BigQuery (if CSV not available)
- Pandas CSV reader
**Flow**: 
1. Try to load from CSV file first
2. Fall back to BigQuery table if CSV doesn't exist
3. Return loaded DataFrame

### `calculate_engagement_score(df: pd.DataFrame) -> pd.Series`
**Purpose**: Calculate normalized engagement score (0-1 scale) based on multiple metrics
**Parameters**:
- `df`: DataFrame with user activity data
**Returns**: Series with engagement scores
**External Tools**: 
- NumPy for numerical computations
- Environment variables for weight configuration
**Flow**:
1. Normalize session frequency, duration, event frequency, and recency
2. Apply configurable weights from environment variables
3. Calculate weighted combination
4. Return normalized scores

### `calculate_revenue_segments(df: pd.DataFrame) -> pd.DataFrame`
**Purpose**: Assign revenue-based user segments using percentile thresholds
**Parameters**:
- `df`: DataFrame with revenue data
**Returns**: DataFrame with revenue segment assignments
**External Tools**: 
- Pandas quantile calculations
- Environment variables for threshold configuration
**Flow**:
1. Calculate revenue percentiles
2. Assign segments: whale (95th+), dolphin (80-95th), minnow (0.1-80th), free_user (0)
3. Add revenue percentile rankings
4. Return updated DataFrame

### `calculate_behavioral_segments(df: pd.DataFrame) -> pd.DataFrame`
**Purpose**: Assign behavioral segments based on engagement patterns
**Parameters**:
- `df`: DataFrame with engagement data
**Returns**: DataFrame with behavioral segment assignments
**External Tools**:
- Pandas quantile calculations
- Environment variables for threshold configuration
**Flow**:
1. Calculate engagement scores
2. Define thresholds based on data distribution
3. Assign segments: high_engagement, moderate_engagement, low_engagement, churned
4. Return updated DataFrame

### `calculate_segment_confidence(df: pd.DataFrame, segment_column: str, segment_value: str) -> float`
**Purpose**: Calculate confidence score for segment assignment
**Parameters**:
- `df`: DataFrame with segment data
- `segment_column`: Name of segment column
- `segment_value`: Specific segment value
**Returns**: Confidence score (0-1)
**External Tools**: 
- Pandas statistical functions
**Flow**:
1. Filter data for specific segment
2. Calculate sample size, data completeness, and metric variance
3. Combine factors with weighted formula
4. Return confidence score

### `calculate_retention_cohorts(df: pd.DataFrame) -> pd.DataFrame`
**Purpose**: Calculate retention cohorts by install date
**Parameters**:
- `df`: DataFrame with cohort data
**Returns**: DataFrame with retention metrics by cohort
**External Tools**: 
- Pandas groupby operations
- Environment variables for minimum sample size
**Flow**:
1. Group by cohort date
2. Calculate retention rates for multiple windows (1, 3, 7, 14, 30, 60 days)
3. Add revenue metrics and statistical significance
4. Return cohort analysis DataFrame

### `calculate_user_journey(df: pd.DataFrame) -> pd.DataFrame`
**Purpose**: Track user progression through key milestones
**Parameters**:
- `df`: DataFrame with user activity data
**Returns**: DataFrame with journey stage assignments
**External Tools**: 
- Pandas datetime operations
**Flow**:
1. Identify available journey stages (FTUE, levels, purchases)
2. Determine highest stage reached per user
3. Calculate time to stage completion
4. Return journey progression DataFrame

### `calculate_journey_funnel(df: pd.DataFrame, journey_df: pd.DataFrame) -> pd.DataFrame`
**Purpose**: Calculate journey funnel conversion rates
**Parameters**:
- `df`: Original user data
- `journey_df`: Journey progression data
**Returns**: DataFrame with funnel metrics
**External Tools**: 
- Pandas aggregation functions
**Flow**:
1. Define funnel stages
2. Calculate users reached at each stage
3. Compute conversion rates and drop-off rates
4. Add timing and significance metrics
5. Return funnel analysis DataFrame

### `save_segment_outputs(df: pd.DataFrame, run_hash: str, segment_definitions: Dict, analysis_report: Dict)`
**Purpose**: Save all segment outputs to files
**Parameters**:
- `df`: Processed DataFrame with segments
- `run_hash`: Run identifier
- `segment_definitions`: Segment metadata
- `analysis_report`: Analysis summary
**External Tools**: 
- Pathlib for file operations
- JSON serialization
- Pandas CSV export
**Flow**:
1. Create output directory structure
2. Generate and save retention cohorts
3. Create behavioral segment summary
4. Create revenue segment summary
5. Save user journey data
6. Save funnel analysis
7. Export segment definitions and analysis report

### `main()`
**Purpose**: Main execution function
**Parameters**: None (uses environment variables)
**Returns**: Boolean success status
**External Tools**: 
- Environment variable access
- All above functions
**Flow**:
1. Load aggregated data from Phase 2
2. Calculate revenue segments
3. Calculate behavioral segments
4. Create segment definitions metadata
5. Generate analysis report
6. Save all outputs
7. Return success status

## Tools & External Dependencies

### Data Processing Tools
- **pandas**: Data manipulation, analysis, aggregation, and CSV I/O operations
- **numpy**: Numerical computations, statistical operations, and array processing
- **scipy**: Statistical functions, significance testing, and scientific computing

### Google Cloud Tools
- **google-cloud-bigquery**: BigQuery client for data retrieval and query execution (fallback when CSV unavailable)
- **google-oauth2**: Authentication and credential management for Google Cloud services

### File System Tools
- **pathlib.Path**: Cross-platform path handling and directory operations
- **json**: JSON serialization, deserialization, and configuration file I/O
- **os**: Environment variables, system operations, and file permissions

### Data Analysis Tools
- **datetime**: Date and time operations, formatting, and calculations
- **collections**: Data structure utilities and specialized containers
- **typing**: Type hints and annotations for better code documentation

## Variables & Configuration

### Input Variables (Function Parameters)
- **`run_hash`**: String identifier for the current analysis run
- **`df`**: Pandas DataFrame containing aggregated user-daily data
- **`segment_definitions`**: Dictionary containing segment configuration and thresholds
- **`analysis_report`**: Dictionary containing analysis results and metadata

### Environment Variables (from .env files)
- **`RUN_HASH`**: Unique identifier for the current run
- **`SEGMENTATION_MINIMUM_SAMPLE_SIZE`**: Minimum sample size for statistical significance (default: 30)
- **`SEGMENTATION_SIGNIFICANCE_THRESHOLD`**: Statistical significance threshold (default: 0.05)
- **`SEGMENTATION_CONFIDENCE_THRESHOLD`**: Confidence threshold for segment assignment (default: 0.85)
- **`CONFIDENCE_SIZE_WEIGHT`**: Weight for sample size in confidence calculation (default: 0.4)
- **`CONFIDENCE_VARIANCE_WEIGHT`**: Weight for variance in confidence calculation (default: 0.4)
- **`CONFIDENCE_COMPLETENESS_WEIGHT`**: Weight for data completeness in confidence calculation (default: 0.2)
- **`GOOGLE_CLOUD_PROJECT`**: Google Cloud project ID for BigQuery access
- **`GOOGLE_APPLICATION_CREDENTIALS`**: Path to Google Cloud service account credentials
- **`DATASET_NAME`**: BigQuery dataset name for data retrieval

### Hardcoded Variables
- **`ENGAGEMENT_WEIGHTS`**: Dictionary defining weights for engagement score calculation
  - `session_frequency_weight`: 0.3
  - `session_duration_weight`: 0.3
  - `event_frequency_weight`: 0.2
  - `recency_weight`: 0.2
- **`REVENUE_SEGMENT_THRESHOLDS`**: Dictionary defining percentile thresholds for revenue segments
  - `whale_threshold`: 90th percentile
  - `dolphin_threshold`: 70th percentile
  - `minnow_threshold`: 30th percentile
- **`BEHAVIORAL_SEGMENT_THRESHOLDS`**: Dictionary defining thresholds for behavioral segments
  - `high_engagement_threshold`: 0.7
  - `low_engagement_threshold`: 0.3
  - `churn_threshold_days`: 7 days since last activity
- **`RETENTION_WINDOWS`**: List of retention analysis windows [0, 1, 3, 7, 14, 30, 60]
- **`JOURNEY_STAGES`**: List of user journey progression stages
- **`OUTPUT_DIRECTORIES`**: List of output subdirectories ['daily', 'cohort', 'user_level']

### Computed Variables
- **`engagement_score`**: Normalized engagement score (0-1) calculated from multiple metrics
- **`revenue_segment`**: User segment based on revenue contribution (whale, dolphin, minnow, free)
- **`behavioral_segment`**: User segment based on engagement patterns (high, medium, low, churned)
- **`segment_confidence`**: Confidence score for segment assignment based on sample size and data quality
- **`retention_rate`**: Percentage of cohort users active on specific days
- **`cohort_size`**: Number of users in each install cohort
- **`time_to_stage_days`**: Days from cohort date to milestone completion
- **`stage_completion_rate`**: Percentage of users completing each journey stage
- **`funnel_conversion_rate`**: Conversion rate between consecutive funnel stages
- **`geographic_metrics`**: Aggregated metrics by country and region
- **`channel_metrics`**: Aggregated metrics by acquisition channel

## Output Files

### Data Files
- `retention_cohorts.csv`: Retention analysis by install cohort
- `behavioral_segment_summary.csv`: Behavioral segment statistics
- `revenue_segment_summary.csv`: Revenue segment statistics
- `user_journey_segments.csv`: Individual user journey progression
- `journey_funnel_analysis.csv`: Funnel conversion analysis

### Metadata Files
- `segment_definitions.json`: Segment criteria and configuration
- `segment_analysis_report.json`: Analysis summary and metrics

## Function Call Flow

```mermaid
graph TD
    A[main] --> B[load_aggregated_data]
    B --> C[calculate_revenue_segments]
    C --> D[calculate_behavioral_segments]
    D --> E[calculate_engagement_score]
    E --> F[calculate_segment_confidence]
    F --> G[calculate_retention_cohorts]
    G --> H[calculate_user_journey]
    H --> I[calculate_journey_funnel]
    I --> J[save_segment_outputs]
    
    J --> K[Save retention_cohorts.csv]
    J --> L[Save behavioral_segment_summary.csv]
    J --> M[Save revenue_segment_summary.csv]
    J --> N[Save user_journey_segments.csv]
    J --> O[Save journey_funnel_analysis.csv]
    J --> P[Save segment_definitions.json]
    J --> Q[Save segment_analysis_report.json]
    
    style A fill:#e1f5fe
    style B fill:#f3e5f5
    style C fill:#e8f5e8
    style D fill:#e8f5e8
    style E fill:#e8f5e8
    style F fill:#fff3e0
    style G fill:#fff3e0
    style H fill:#fff3e0
    style I fill:#fff3e0
    style J fill:#e8f5e8
```

## Usage Examples

### Basic Usage
```bash
# Run as part of full workflow
python scripts/analysis_workflow_orchestrator.py

# Run segmentation only
python scripts/analysis_workflow_orchestrator.py --mode segmentation-only
```

### Environment Configuration
```bash
# Set segmentation parameters
export SEGMENTATION_MINIMUM_SAMPLE_SIZE=50
export WHALE_REVENUE_PERCENTILE=0.9
export CHURN_DAYS_THRESHOLD=21
```

## Error Handling
- **Data Loading**: Graceful fallback from CSV to BigQuery
- **Missing Data**: Handles null values in calculations
- **Sample Size**: Validates minimum sample sizes for statistical significance
- **File Operations**: Creates directories and handles file write errors

## Performance Considerations
- **Memory Usage**: Processes data in chunks for large datasets
- **Statistical Calculations**: Uses efficient pandas operations
- **File I/O**: Batches file operations for better performance

## Integration Points
- **Input**: Aggregated data from Phase 2 (`data_aggregation_v3.py`)
- **Output**: Segment data for Phase 5 (LLM Insights Generation)
- **Orchestration**: Called by `analysis_workflow_orchestrator.py` in Phase 3
