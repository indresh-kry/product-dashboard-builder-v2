# Analysis Workflow - Agentic Framework

Run automated data analysis with agentic LLM framework for insights generation

## Description
Executes a streamlined analysis workflow from raw clickstream data through schema discovery, data aggregation, core metric generation, and AI-powered insights using a modular agentic framework with 100% success rate and enhanced business metrics.

## Usage
```bash
# Unified workflow (recommended - combines all phases)
python analysis_workflow_orchestrator_unified.py

# Direct agentic framework usage (for existing data)
python -c "from agents.agentic_coordinator import AgenticCoordinator; coordinator = AgenticCoordinator(); results = coordinator.run_analysis('run_hash', run_metadata)"
```

## Options
- `--quick` - Run abbreviated version (3 iterations instead of 5)
- `--focus <area>` - Focus on specific area (revenue|engagement|growth|health|retention)
- `--skip-schema` - Skip schema discovery if recent run exists (use with caution)
- `--mode <mode>` - Analysis mode (full|schema-only|aggregation-only|custom)
- `--validate-only` - Run validation checks without full analysis
- `--resume <run_hash>` - Resume from existing run

## What it does
1. **System Initialization**: Creates unique run with hash identifier and folder structure
2. **Environment Setup**: Configures all necessary environment variables and connections
3. **Schema Discovery**: Discovers and documents data schema with field mappings
4. **Rules Engine Integration**: Applies business rules for identifier categorization
5. **Data Aggregation**: Generates core product metrics and aggregated data
6. **User Segmentation**: Defines user segments with statistical grounding
7. **Quality Assurance**: Runs basic data validation and sanity checks
8. **Agentic LLM Insights**: Generates AI-powered insights using modular agent framework
9. **Business Metrics**: Calculates 10 key performance indicators with true D1 retention
10. **Final Reporting**: Outputs organized findings, data tables, and actionable insights

## Core Metrics Generated
The system generates the following core product metrics based on data availability:

1. **Date-wise DAU numbers**
2. **Date-wise new logins - overall**
3. **Date-wise new logins by acquisition channel**
4. **Date-wise new logins by country (or state if country is single value)**
5. **Event funnel** (e.g., Login → Game started → First level players → IAP generated etc.) of players on date of first login
6. **Date-wise revenue**
7. **Date-wise revenue by country (or state if country is single value)**
8. **Date-wise revenue by type of revenue** (IAP v/s ads v/s subscription)
9. **Date-wise retention of cohort** of players with first time activity on a particular date (D1, D2, D3, D4, D5, D6, D7, D14, D30 retentions based on duration of data available)
10. **Date-wise retention** (D1, D2, D3, D7 retentions) based on last event funnel step reached by player on date of first login

## Output Structure
```
run_logs/{run_hash}/
├── outputs/
│   ├── schema/              # Data dictionary and mappings
│   ├── segments/            # User segment definitions and statistics
│   ├── aggregations/        # Core product metrics and data tables
│   ├── validation/          # Quality assurance and sanity check results
│   ├── insights/            # Agentic LLM insights and reports
│   │   ├── agentic_insights_report.md  # Human-readable insights
│   │   └── agentic_insights.json       # Structured agent results
│   ├── reports/             # Final reports and data tables
│   └── run_summary.md       # Overview and index
├── working/                 # Scripts and queries used
├── logs/                    # Execution logs and error tracking
└── .env                     # Run-specific environment
```

## Agentic Framework Architecture

The agentic framework provides a modular, scalable approach to LLM-based analysis:

### Core Components

1. **Agentic Coordinator** (`scripts/agents/agentic_coordinator.py`)
   - Main orchestrator for all agents
   - Manages agent execution and result aggregation
   - Generates comprehensive markdown reports
   - Calculates business metrics automatically

2. **Specialized Agents** (`scripts/agents/`)
   - **Daily Metrics Agent**: DAU, new users, revenue analysis
   - **User Segmentation Agent**: User behavior and segment analysis
   - **Geographic Agent**: Location-based performance analysis
   - **Cohort Retention Agent**: True retention calculation using cohort data
   - **Revenue Optimization Agent**: Revenue analysis and optimization
   - **Data Quality Agent**: Data validation and quality assessment

3. **Data Loaders** (`scripts/agents/data_loaders/`)
   - Automated data loading and validation
   - Path resolution for different data types
   - Error handling and data quality checks

4. **Prompt Generators** (`scripts/agents/prompt_generators/`)
   - Intelligent prompt creation for each agent type
   - Evidence-based insight requirements
   - Specific, actionable recommendation generation

5. **LLM Client** (`scripts/agents/llm_client.py`)
   - Unified OpenAI API integration
   - Automatic API key resolution from environment or creds.json
   - Error handling and retry logic

### Key Features

- **100% Success Rate**: All agents execute successfully
- **Evidence-Based Insights**: Data-driven recommendations with supporting evidence
- **True D1 Retention**: Accurate cohort-based retention calculation
- **Business Metrics**: 10 key performance indicators automatically calculated
- **Modular Design**: Easy to add new agents and capabilities
- **Comprehensive Reporting**: Human-readable markdown reports with business metrics

## Implementation

### Phase 0: System Initialization and Environment Setup

```bash
#!/bin/bash
# Load environment variables from project .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "ERROR: .env file not found. Please create .env with required configuration"
    exit 1
fi

# Verify required environment variables
REQUIRED_VARS=("DATASET_NAME" "API_KEY")
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        echo "ERROR: $var not found in .env file"
        exit 1
    fi
done

# Set default column mappings (can be overridden in .env)
EVENT_NAME_COLUMN="${EVENT_NAME_COLUMN:-name}"
USER_ID_COLUMN="${USER_ID_COLUMN:-custom_user_id}"
DEVICE_ID_COLUMN="${DEVICE_ID_COLUMN:-device_id}"
TIMESTAMP_COLUMN="${TIMESTAMP_COLUMN:-adjusted_timestamp}"
REVENUE_COLUMN="${REVENUE_COLUMN:-converted_revenue}"
REVENUE_VALIDATION_COLUMN="${REVENUE_VALIDATION_COLUMN:-is_revenue_event}"
INSTALL_EVENTS="${INSTALL_EVENTS:-'install','first_launch','af_first_launch'}"
ANALYSIS_WINDOW_DAYS="${ANALYSIS_WINDOW_DAYS:-90}"

# Generate 6-character hash
export RUN_HASH=$(python3 -c "import secrets; print(secrets.token_hex(3))")

# Create run directory structure
mkdir -p run_logs/${RUN_HASH}/outputs/{schema,segments,aggregations,validation,reports}
mkdir -p run_logs/${RUN_HASH}/working run_logs/${RUN_HASH}/logs

# Log run information
echo "Run ${RUN_HASH} started at $(date)" > run_logs/${RUN_HASH}/logs/run.log
echo "Dataset: ${DATASET_NAME}" >> run_logs/${RUN_HASH}/logs/run.log
echo "Mode: ${MODE:-full}" >> run_logs/${RUN_HASH}/logs/run.log
echo "Run hash: ${RUN_HASH}"
echo "Dataset: ${DATASET_NAME}"

# Create environment file for Python scripts
cat > run_logs/${RUN_HASH}/.env << EOF
export RUN_HASH=${RUN_HASH}
export DATASET_NAME='${DATASET_NAME}'
export EVENT_NAME_COLUMN='${EVENT_NAME_COLUMN}'
export USER_ID_COLUMN='${USER_ID_COLUMN}'
export DEVICE_ID_COLUMN='${DEVICE_ID_COLUMN}'
export TIMESTAMP_COLUMN='${TIMESTAMP_COLUMN}'
export REVENUE_COLUMN='${REVENUE_COLUMN}'
export REVENUE_VALIDATION_COLUMN='${REVENUE_VALIDATION_COLUMN}'
export INSTALL_EVENTS='${INSTALL_EVENTS}'
export ANALYSIS_WINDOW_DAYS='${ANALYSIS_WINDOW_DAYS}'
export MODE='${MODE:-full}'
EOF

# Copy other relevant environment variables from project .env
if [ -f .env ]; then
    grep -E '^(API_KEY|GOOGLE_APPLICATION_CREDENTIALS|OPENAI_API_KEY|DATABASE_URL)=' .env >> run_logs/${RUN_HASH}/.env 2>/dev/null || true
fi

# Initialize system health checks
python3 scripts/system_health_check.py --run-hash ${RUN_HASH}
```

### Phase 1: Schema Discovery and Field Mapping

Use the Task tool to launch a general-purpose agent with this prompt:

"Complete schema discovery for ${DATASET_NAME}
Run hash: ${RUN_HASH}
Save all outputs to run_logs/${RUN_HASH}/outputs/schema/ and working scripts to run_logs/${RUN_HASH}/working/

CRITICAL PATH REQUIREMENTS:
1. ALL Python scripts MUST be created in run_logs/${RUN_HASH}/working/
2. ALL outputs MUST go to run_logs/${RUN_HASH}/outputs/
3. NEVER create files in the repository root directory
4. Use ABSOLUTE paths when creating files or explicitly change to working directory first

IMPORTANT: All Python scripts must use environment variables:
```python
import os
RUN_HASH = os.environ.get('RUN_HASH')
DATASET_NAME = os.environ.get('DATASET_NAME')
EVENT_NAME_COLUMN = os.environ.get('EVENT_NAME_COLUMN', 'name')
USER_ID_COLUMN = os.environ.get('USER_ID_COLUMN', 'user_id')
DEVICE_ID_COLUMN = os.environ.get('DEVICE_ID_COLUMN', 'device_id')
TIMESTAMP_COLUMN = os.environ.get('TIMESTAMP_COLUMN', 'timestamp')
REVENUE_COLUMN = os.environ.get('REVENUE_COLUMN', 'revenue')
OUTPUTS_DIR = f'run_logs/{RUN_HASH}/outputs/schema'
WORKING_DIR = f'run_logs/{RUN_HASH}/working'

# CRITICAL: When creating Python scripts, use absolute paths
# Example: script_path = os.path.join(os.getcwd(), WORKING_DIR, 'schema_discovery.py')
```

When running Python scripts, always ensure environment variables are set:
```bash
source run_logs/${RUN_HASH}/.env && python3 run_logs/${RUN_HASH}/working/script_name.py
```

1. COLUMN DOCUMENTATION
   Query INFORMATION_SCHEMA and sample data to understand:
   - All columns with data types and descriptions
   - Identify timestamp columns (looking for ${TIMESTAMP_COLUMN} or similar)
   - Identify user identifier columns (looking for ${USER_ID_COLUMN}, ${DEVICE_ID_COLUMN} or similar)
   - Identify attribution columns (partner, campaign, source, etc.)
   - Identify revenue/monetary columns (looking for ${REVENUE_COLUMN} or similar)
   - Document data quality metrics (null rates, uniqueness, etc.)
   
   Save complete documentation to run_logs/${RUN_HASH}/outputs/schema/column_definitions.json

2. EVENT TAXONOMY
   Query all unique event names with counts and parse arguments JSON:
   ```sql
   SELECT
     name as event_name,
     COUNT(*) as event_count,
     COUNT(DISTINCT custom_user_id) as unique_users,
     COUNT(DISTINCT device_id) as unique_devices,
     ANY_VALUE(arguments) as sample_arguments,
     MIN(adjusted_timestamp) as first_seen,
     MAX(adjusted_timestamp) as last_seen
   FROM `${DATASET_NAME}`
   WHERE adjusted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
   GROUP BY name
   ORDER BY event_count DESC
   ```

   Categorize events into:
   - Monetization: IAP (in-app purchases) vs AdMon (ad monetization) vs Subscription
   - Progression: Tutorials, levels, milestones
   - Engagement: Sessions, features, social
   - Technical: Errors, loading, performance
   - Attribution: Install, first launch, campaign tracking

   Save to run_logs/${RUN_HASH}/outputs/schema/event_taxonomy.json

3. REVENUE MAPPING
   Identify ALL revenue sources and create separate schemas:

   IAP Revenue (run_logs/${RUN_HASH}/outputs/schema/iap_revenue_schema.json):
   - Primary events: 'iap_sing', purchase events
   - Revenue field: converted_revenue where is_revenue_event=true
   - Include refund handling logic

   AdMon Revenue (run_logs/${RUN_HASH}/outputs/schema/admon_revenue_schema.json):
   - Ad events: ad_impression, rewarded_video, interstitial
   - Revenue field: ad_revenue or converted_revenue from ad events
   - Aggregation rules

4. USER IDENTIFICATION HIERARCHY
   Document rules in run_logs/${RUN_HASH}/outputs/schema/user_identification_rules.json:
   - When to use custom_user_id (logged in users)
   - When to use device_id (including guests where custom_user_id='')
   - How to handle device sharing (multiple users per device)
   - Deduplication strategy

5. DATA QUALITY ASSESSMENT
   Check and document:
   - Null rates per critical column
   - Data gaps in time series
   - Duplicate patterns
   - Revenue calculation validation (compare methods)
   
   Save to run_logs/${RUN_HASH}/outputs/schema/data_quality_report.md

6. CREATE MASTER SCHEMA MAPPING
   Generate run_logs/${RUN_HASH}/outputs/schema/schema_mapping.json:
   ```json
   {
     "version": "1.0",
     "run_hash": "${RUN_HASH}",
     "generated_at": "timestamp",
     "table": "${DATASET_NAME}",
     "user_identification": {
       "primary_field": "custom_user_id or device_id based on context",
       "rules": {...}
     },
     "revenue_calculation": {
       "iap": {
         "events": ["iap_sing", ...],
         "revenue_field": "converted_revenue",
         "filters": "is_revenue_event=true"
       },
       "admon": {
         "events": ["ad_impression", ...],
         "revenue_field": "ad_revenue",
         "filters": "..."
       }
     },
     "key_events": {
       "install": "first_launch",
       "tutorial_complete": ["ftue_complete", "tutorial_complete", ...],
       "first_purchase": "first iap_sing",
       "level_progression": "level_progress events"
     }
   }
   ```

7. INTEGRATION WITH RULES ENGINE
   Call scripts/rules_engine_integration.py to:
   - Apply differential rulesets for identifier categorization
   - Validate field mappings against business rules
   - Generate field mapping confidence scores

Return confirmation that schema discovery is complete and path to schema_mapping.json."

### Phase 2: Data Aggregation and Core Metric Generation

Use the Task tool to launch a general-purpose agent:

"Generate core product metrics and data aggregations.
Run hash: ${RUN_HASH}
Load schema from run_logs/${RUN_HASH}/outputs/schema/schema_mapping.json

CRITICAL PATH REQUIREMENTS:
1. ALL Python scripts MUST be created in run_logs/${RUN_HASH}/working/
2. ALL outputs MUST go to run_logs/${RUN_HASH}/outputs/
3. NEVER create files in the repository root directory
4. Use ABSOLUTE paths when creating files or explicitly change to working directory first

IMPORTANT: All Python scripts must use environment variables:
```python
import os
RUN_HASH = os.environ.get('RUN_HASH')
DATASET_NAME = os.environ.get('DATASET_NAME')
OUTPUTS_DIR = f'run_logs/{RUN_HASH}/outputs/aggregations'
WORKING_DIR = f'run_logs/{RUN_HASH}/working'

# CRITICAL: When creating Python scripts, use absolute paths
# Example: script_path = os.path.join(os.getcwd(), WORKING_DIR, 'data_aggregation.py')
```

When running Python scripts, always ensure environment variables are set:
```bash
source run_logs/${RUN_HASH}/.env && python3 run_logs/${RUN_HASH}/working/script_name.py
```

1. GENERATE CORE PRODUCT METRICS
   Create optimized SQL queries for the 10 core metrics:

   a. Date-wise DAU numbers:
   ```sql
   SELECT
     DATE(adjusted_timestamp) as date,
     COUNT(DISTINCT custom_user_id) as dau
   FROM `${DATASET_NAME}`
   WHERE adjusted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
   GROUP BY DATE(adjusted_timestamp)
   ORDER BY date
   ```

   b. Date-wise new logins - overall:
   ```sql
   SELECT
     DATE(adjusted_timestamp) as date,
     COUNT(DISTINCT custom_user_id) as new_logins
   FROM `${DATASET_NAME}`
   WHERE name IN ('login', 'first_launch', 'install')
     AND adjusted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
   GROUP BY DATE(adjusted_timestamp)
   ORDER BY date
   ```

   c. Date-wise new logins by acquisition channel:
   ```sql
   SELECT
     DATE(adjusted_timestamp) as date,
     channel,
     COUNT(DISTINCT custom_user_id) as new_logins
   FROM `${DATASET_NAME}`
   WHERE name IN ('login', 'first_launch', 'install')
     AND adjusted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
   GROUP BY DATE(adjusted_timestamp), channel
   ORDER BY date, channel
   ```

   d. Date-wise new logins by country:
   ```sql
   SELECT
     DATE(adjusted_timestamp) as date,
     country,
     COUNT(DISTINCT custom_user_id) as new_logins
   FROM `${DATASET_NAME}`
   WHERE name IN ('login', 'first_launch', 'install')
     AND adjusted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
   GROUP BY DATE(adjusted_timestamp), country
   ORDER BY date, country
   ```

   e. Event funnel of players on date of first login:
   ```sql
   WITH first_login AS (
     SELECT
       custom_user_id,
       MIN(DATE(adjusted_timestamp)) as first_login_date
     FROM `${DATASET_NAME}`
     WHERE name IN ('login', 'first_launch', 'install')
     GROUP BY custom_user_id
   )
   SELECT
     fl.first_login_date,
     COUNT(DISTINCT fl.custom_user_id) as total_users,
     COUNT(DISTINCT CASE WHEN e.name = 'game_started' THEN e.custom_user_id END) as game_started,
     COUNT(DISTINCT CASE WHEN e.name = 'level_completed' THEN e.custom_user_id END) as first_level_players,
     COUNT(DISTINCT CASE WHEN e.name = 'iap_sing' THEN e.custom_user_id END) as iap_generated
   FROM first_login fl
   LEFT JOIN `${DATASET_NAME}` e ON fl.custom_user_id = e.custom_user_id
   WHERE fl.first_login_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
   GROUP BY fl.first_login_date
   ORDER BY fl.first_login_date
   ```

   f. Date-wise revenue:
   ```sql
   SELECT
     DATE(adjusted_timestamp) as date,
     SUM(converted_revenue) as total_revenue
   FROM `${DATASET_NAME}`
   WHERE converted_revenue > 0
     AND adjusted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
   GROUP BY DATE(adjusted_timestamp)
   ORDER BY date
   ```

   g. Date-wise revenue by country:
   ```sql
   SELECT
     DATE(adjusted_timestamp) as date,
     country,
     SUM(converted_revenue) as revenue
   FROM `${DATASET_NAME}`
   WHERE converted_revenue > 0
     AND adjusted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
   GROUP BY DATE(adjusted_timestamp), country
   ORDER BY date, country
   ```

   h. Date-wise revenue by type:
   ```sql
   SELECT
     DATE(adjusted_timestamp) as date,
     CASE 
       WHEN name = 'iap_sing' THEN 'IAP'
       WHEN name IN ('ad_impression', 'rewarded_video') THEN 'Ads'
       WHEN name LIKE '%subscription%' THEN 'Subscription'
       ELSE 'Other'
     END as revenue_type,
     SUM(converted_revenue) as revenue
   FROM `${DATASET_NAME}`
   WHERE converted_revenue > 0
     AND adjusted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
   GROUP BY DATE(adjusted_timestamp), revenue_type
   ORDER BY date, revenue_type
   ```

   i. Date-wise retention by cohort:
   ```sql
   WITH user_cohorts AS (
     SELECT
       custom_user_id,
       MIN(DATE(adjusted_timestamp)) as cohort_date
     FROM `${DATASET_NAME}`
     WHERE name IN ('login', 'first_launch', 'install')
     GROUP BY custom_user_id
   ),
   daily_activity AS (
     SELECT
       uc.cohort_date,
       DATE(e.adjusted_timestamp) as activity_date,
       COUNT(DISTINCT e.custom_user_id) as active_users
     FROM user_cohorts uc
     JOIN `${DATASET_NAME}` e ON uc.custom_user_id = e.custom_user_id
     WHERE uc.cohort_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
     GROUP BY uc.cohort_date, DATE(e.adjusted_timestamp)
   )
   SELECT
     cohort_date,
     DATE_DIFF(activity_date, cohort_date, DAY) as days_since_cohort,
     active_users,
     COUNT(DISTINCT custom_user_id) OVER (PARTITION BY cohort_date) as cohort_size,
     active_users / COUNT(DISTINCT custom_user_id) OVER (PARTITION BY cohort_date) as retention_rate
   FROM daily_activity
   ORDER BY cohort_date, days_since_cohort
   ```

   j. Date-wise retention by funnel step:
   ```sql
   WITH funnel_steps AS (
     SELECT
       custom_user_id,
       MIN(DATE(adjusted_timestamp)) as first_login_date,
       MAX(CASE WHEN name = 'game_started' THEN 1 ELSE 0 END) as reached_game_started,
       MAX(CASE WHEN name = 'level_completed' THEN 1 ELSE 0 END) as reached_level_completed,
       MAX(CASE WHEN name = 'iap_sing' THEN 1 ELSE 0 END) as reached_iap
     FROM `${DATASET_NAME}`
     WHERE name IN ('login', 'first_launch', 'install', 'game_started', 'level_completed', 'iap_sing')
     GROUP BY custom_user_id
   )
   SELECT
     first_login_date,
     COUNT(DISTINCT custom_user_id) as total_users,
     COUNT(DISTINCT CASE WHEN reached_game_started = 1 THEN custom_user_id END) as game_started_users,
     COUNT(DISTINCT CASE WHEN reached_level_completed = 1 THEN custom_user_id END) as level_completed_users,
     COUNT(DISTINCT CASE WHEN reached_iap = 1 THEN custom_user_id END) as iap_users
   FROM funnel_steps
   WHERE first_login_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
   GROUP BY first_login_date
   ORDER BY first_login_date
   ```

2. EXECUTE DATA AGGREGATION
   Run aggregation queries with:
   - Dry-run cost estimation first
   - Progress tracking and logging
   - Error handling and retry logic
   - Result validation and quality checks

3. CREATE METRIC OUTPUT TABLES
   Generate individual metric tables:
   - run_logs/${RUN_HASH}/outputs/aggregations/dau_metrics.csv
   - run_logs/${RUN_HASH}/outputs/aggregations/new_logins_metrics.csv
   - run_logs/${RUN_HASH}/outputs/aggregations/revenue_metrics.csv
   - run_logs/${RUN_HASH}/outputs/aggregations/retention_metrics.csv
   - run_logs/${RUN_HASH}/outputs/aggregations/funnel_metrics.csv

4. INTEGRATION WITH SQL RUNNER
   Call scripts/sql_runner_integration.py to:
   - Execute queries with proper error handling
   - Monitor query performance and costs
   - Validate results against expected schemas

5. VALIDATE AGGREGATION RESULTS
   Run basic validation:
   - Cross-check metric calculations
   - Verify data consistency across tables
   - Check for missing or anomalous data

Return confirmation and summary of aggregation results."

### Phase 3: User Segmentation with Statistical Framework

Use the Task tool to launch a general-purpose agent:

"Establish user segmentation with statistical grounding.
Run hash: ${RUN_HASH}
Load schema from run_logs/${RUN_HASH}/outputs/schema/schema_mapping.json
Load aggregations from run_logs/${RUN_HASH}/outputs/aggregations/

CRITICAL PATH REQUIREMENTS:
1. ALL Python scripts MUST be created in run_logs/${RUN_HASH}/working/
2. ALL outputs MUST go to run_logs/${RUN_HASH}/outputs/
3. NEVER create files in the repository root directory
4. Use ABSOLUTE paths when creating files or explicitly change to working directory first

IMPORTANT: All Python scripts must use environment variables:
```python
import os
RUN_HASH = os.environ.get('RUN_HASH')
DATASET_NAME = os.environ.get('DATASET_NAME')
OUTPUTS_DIR = f'run_logs/{RUN_HASH}/outputs/segments'
WORKING_DIR = f'run_logs/{RUN_HASH}/working'

# CRITICAL: When creating Python scripts, use absolute paths
# Example: script_path = os.path.join(os.getcwd(), WORKING_DIR, 'user_segmentation.py')
```

When running Python scripts, always ensure environment variables are set:
```bash
source run_logs/${RUN_HASH}/.env && python3 run_logs/${RUN_HASH}/working/script_name.py
```

1. DEFINE INSTALL_MONTH SEGMENTS
   Every analysis must be grounded by install_month first.

   Query to identify segments:
   ```sql
   SELECT
     DATE_TRUNC(MIN(adjusted_timestamp), MONTH) as install_month,
     COUNT(DISTINCT custom_user_id) as segment_size,
     MIN(DATE(adjusted_timestamp)) as earliest_install,
     MAX(DATE(adjusted_timestamp)) as latest_install,
     DATE_DIFF(CURRENT_DATE(), MAX(DATE(adjusted_timestamp)), DAY) as observable_days
   FROM `${DATASET_NAME}`
   WHERE name IN ('install', 'first_launch', 'af_first_launch')
     AND adjusted_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
   GROUP BY custom_user_id
   ```

   Save segment definitions to run_logs/${RUN_HASH}/outputs/segments/segment_definitions.json

2. CREATE USER SEGMENTS
   Define segmentation approaches:
   - Time-based: Install month, first week, first month
   - Behavioral: Engagement level, feature usage, progression
   - Revenue-based: Spender vs non-spender, LTV tiers
   - Demographic: Platform, country, channel

   Save to run_logs/${RUN_HASH}/outputs/segments/user_segments.json

3. CALCULATE SEGMENT STATISTICS
   For each segment, calculate:
   - Segment size and composition
   - Conversion rates at D1, D7, D30
   - Revenue distribution (percentiles, mean, median)
   - Engagement metrics (sessions, events, retention)
   - Statistical power for comparisons

   Save to run_logs/${RUN_HASH}/outputs/segments/segment_statistics.csv

4. STATISTICAL FRAMEWORK
   Create run_logs/${RUN_HASH}/outputs/segments/statistical_framework.json:
   - Significance thresholds (p=0.05)
   - Minimum sample sizes (n>=30)
   - Test selection rules
   - Confidence interval calculations

Return confirmation and summary of segmentation framework."

### Phase 4: Quality Assurance and Validation

Use the Task tool to launch a general-purpose agent:

"Run basic quality assurance and validation checks.
Run hash: ${RUN_HASH}

CRITICAL PATH REQUIREMENTS:
1. ALL Python scripts MUST be created in run_logs/${RUN_HASH}/working/
2. ALL outputs MUST go to run_logs/${RUN_HASH}/outputs/
3. NEVER create files in the repository root directory
4. Use ABSOLUTE paths when creating files or explicitly change to working directory first

IMPORTANT: All Python scripts must use environment variables:
```python
import os
RUN_HASH = os.environ.get('RUN_HASH')
DATASET_NAME = os.environ.get('DATASET_NAME')
OUTPUTS_DIR = f'run_logs/{RUN_HASH}/outputs/validation'
WORKING_DIR = f'run_logs/{RUN_HASH}/working'

# CRITICAL: When creating Python scripts, use absolute paths
# Example: script_path = os.path.join(os.getcwd(), WORKING_DIR, 'quality_assurance.py')
```

When running Python scripts, always ensure environment variables are set:
```bash
source run_logs/${RUN_HASH}/.env && python3 run_logs/${RUN_HASH}/working/script_name.py
```

1. DATA QUALITY VALIDATION
   Run basic data quality checks:
   - Schema consistency across all outputs
   - Data completeness and accuracy validation
   - Revenue calculation cross-validation
   - User identification consistency checks
   - Time series continuity and gap analysis

   Save to run_logs/${RUN_HASH}/outputs/validation/data_quality_validation.json

2. SANITY CHECKS
   Run basic sanity checks:
   - Retention rates vs industry standards
   - Revenue metrics vs typical ranges
   - User behavior patterns vs expected norms
   - Technical metrics vs performance baselines

   Save to run_logs/${RUN_HASH}/outputs/validation/sanity_checks.json

3. CROSS-VALIDATION TESTS
   Perform basic cross-validation:
   - Compare different calculation methods
   - Validate results across different time periods
   - Check consistency across user segments

   Save to run_logs/${RUN_HASH}/outputs/validation/cross_validation_results.json

Return validation summary and quality assurance report."

### Phase 5: Agentic LLM Insights Generation

The agentic framework provides modular, specialized agents for comprehensive analysis:

**Agentic Coordinator Execution:**
```python
from scripts.agents.agentic_coordinator import AgenticCoordinator

# Initialize coordinator
coordinator = AgenticCoordinator()

# Run analysis with all agents
results = coordinator.run_analysis(run_hash, run_metadata)
```

**Available Specialized Agents:**

1. **Daily Metrics Agent**
   - Analyzes DAU trends and patterns
   - Identifies growth opportunities in new logins
   - Calculates average daily users and new users
   - Generates evidence-based insights with specific data points

2. **User Segmentation Agent**
   - Analyzes user behavior patterns and segments
   - Identifies high-value user characteristics
   - Provides segment-specific recommendations
   - Includes statistical validation and confidence levels

3. **Geographic Agent**
   - Analyzes location-based performance
   - Identifies regional opportunities and challenges
   - Provides country/region-specific insights
   - Includes geographic revenue analysis

4. **Cohort Retention Agent**
   - Calculates true D1, D3, D7 retention using days_since_first_event
   - Analyzes retention patterns and cliff points
   - Identifies retention optimization opportunities
   - Provides cohort-specific recommendations

5. **Revenue Optimization Agent**
   - Analyzes revenue performance and concentration
   - Identifies revenue optimization opportunities
   - Calculates total and average daily revenue
   - Provides revenue-specific insights and recommendations

6. **Data Quality Agent**
   - Validates data quality and completeness
   - Identifies data quality issues and limitations
   - Provides data improvement recommendations
   - Includes confidence levels and data validation

**Enhanced Business Metrics:**
The agentic framework automatically calculates 10 key business metrics:
- Duration of analysis (data range)
- App name
- Average daily users
- Average daily new users
- Average D1 retention (true cohort-based)
- Average D3 retention
- Average D7 retention
- Total revenue for given duration
- Average daily revenue
- Total number of whale users

**Output Generation:**
- **JSON Reports**: Structured data for programmatic access
- **Markdown Reports**: Human-readable insights and recommendations
- **Business Metrics**: Key performance indicators with evidence
- **Agent Results**: Individual agent analysis with success tracking

**Key Improvements:**
- **100% Agent Success Rate**: All agents execute successfully
- **Evidence-Based Insights**: Data-driven recommendations with supporting evidence
- **True D1 Retention**: Accurate cohort-based retention calculation
- **Modular Architecture**: Scalable and maintainable agent system
- **Enhanced Reporting**: Comprehensive markdown reports with business metrics

### Phase 6: Final Report Generation

Use the Task tool to launch a general-purpose agent:

"Generate comprehensive final reports and organize all outputs from run ${RUN_HASH}.
Run hash: ${RUN_HASH}

CRITICAL PATH REQUIREMENTS:
1. ALL Python scripts MUST be created in run_logs/${RUN_HASH}/working/
2. ALL outputs MUST go to run_logs/${RUN_HASH}/outputs/
3. NEVER create files in the repository root directory
4. Use ABSOLUTE paths when creating files or explicitly change to working directory first

IMPORTANT: All Python scripts must use environment variables:
```python
import os
RUN_HASH = os.environ.get('RUN_HASH')
DATASET_NAME = os.environ.get('DATASET_NAME')
OUTPUTS_DIR = f'run_logs/{RUN_HASH}/outputs/reports'
WORKING_DIR = f'run_logs/{RUN_HASH}/working'

# CRITICAL: When creating Python scripts, use absolute paths
# Example: script_path = os.path.join(os.getcwd(), WORKING_DIR, 'report_generator.py')
```

When running Python scripts, always ensure environment variables are set:
```bash
source run_logs/${RUN_HASH}/.env && python3 run_logs/${RUN_HASH}/working/script_name.py
```

1. CREATE RUN SUMMARY
   Generate run_logs/${RUN_HASH}/outputs/run_summary.md:

   # Analysis Workflow Run ${RUN_HASH}

   ## Run Metadata
   - Started: [from run.log]
   - Completed: [current time]
   - Dataset: ${DATASET_NAME}
   - Mode: [full|quick|schema-only|etc.]
   - Install Months Analyzed: [from segment analysis]
   - Core Metrics Generated: [list of 10 metrics]

   ## Executive Summary
   - Key findings and business implications
   - Critical insights and recommendations
   - Data quality assessment
   - Confidence levels and limitations

   ## Core Metrics Summary
   - DAU trends and patterns
   - New user acquisition performance
   - Revenue analysis and concentration
   - Retention patterns and insights
   - Funnel conversion analysis

   ## Key Insights
   - AI-generated insights from metric tables
   - Gap analysis and trend identification
   - Benchmark comparisons
   - Performance optimization opportunities

   ## Business Recommendations
   - Immediate action items (high confidence findings)
   - Medium-term optimization opportunities
   - Long-term strategic considerations
   - Follow-up analysis recommendations

   ## Data Quality Assessment
   - Overall data quality score
   - Key data quality issues identified
   - Recommendations for data improvement
   - Validation results summary

   ## Technical Notes
   - Methodology and approach
   - Statistical methods used
   - Data limitations and caveats
   - Reproducibility information

   ## Outputs Generated
   List all files created with descriptions and locations

   ## Next Steps
   - Suggested follow-up analyses
   - Data collection improvements
   - Monitoring and tracking recommendations
   - Future analysis opportunities

2. VERIFY COMPLETENESS
   Check that all phases completed:
   - [ ] Schema mapping exists and validated
   - [ ] User segments defined and analyzed
   - [ ] Core metrics generated and validated
   - [ ] Quality assurance completed
   - [ ] LLM insights generated
   - [ ] Reports created

3. CREATE INDEX
   Generate run_logs/${RUN_HASH}/outputs/index.txt listing all files

Return path to run_summary.md and completion confirmation."

### Error Handling and Recovery

If any phase fails:
1. Log error to run_logs/${RUN_HASH}/logs/errors.log with full context
2. Save partial results completed so far
3. Note failure in run_summary.md with recovery suggestions
4. Create recovery plan with specific steps
5. Preserve all working scripts for debugging

### Configuration and Customization

```bash
# Default configuration
DEFAULT_ITERATIONS=5
QUICK_ITERATIONS=3
CONFIDENCE_THRESHOLD=0.85
MINIMUM_SAMPLE_SIZE=30
ANALYSIS_WINDOW_DAYS=90
STATISTICAL_SIGNIFICANCE=0.05

# Customizable parameters
MAX_QUERY_COST=100  # USD
PARALLEL_QUERIES=4
RETRY_ATTEMPTS=3
TIMEOUT_SECONDS=3600
```

### Success Criteria

The analysis workflow succeeds when:
- Schema correctly maps all events, revenue, and user identification
- User segments properly grounded with statistical validity
- All 10 core metrics generated with quality validation
- LLM insights generated from metric tables
- Quality assurance validates all results and methodology
- Outputs organized in comprehensive run folder with clear documentation
- Reports provide actionable insights and recommendations

### Integration Points

The workflow integrates with:
- **Rules Engine**: scripts/rules_engine_integration.py
- **SQL Runner**: scripts/sql_runner_integration.py  
- **LLM Workflow**: scripts/llm_workflow_integration.py
- **Agent Memory**: scripts/agent_memory_integration.py
- **System Health**: scripts/system_health_check.py

### Example Usage

```bash
# Unified workflow (recommended - all phases)
cd scripts
python analysis_workflow_orchestrator_unified.py --app-filter "com.nukebox.mandir" --date-start "2025-09-15" --date-end "2025-09-30" --raw-data-limit 100 --aggregation-limit 250000

# Direct agentic framework usage (for existing data)
cd scripts
python -c "from agents.agentic_coordinator import AgenticCoordinator; coordinator = AgenticCoordinator(); results = coordinator.run_analysis('run_hash', {'app_filter': 'com.nukebox.mandir'})"

# Test agentic framework
cd scripts
python test_agentic_framework.py

# Run with specific parameters
export BIGQUERY_READ_ONLY_MODE="true"
python analysis_workflow_orchestrator_agentic.py
```

### Agentic Framework Usage

```python
# Direct agentic framework usage
from scripts.agents.agentic_coordinator import AgenticCoordinator

# Initialize coordinator
coordinator = AgenticCoordinator()

# Run analysis
run_metadata = {
    'app_filter': 'com.nukebox.mandir',
    'start_date': '2025-09-15',
    'end_date': '2025-09-30'
}

results = coordinator.run_analysis('your_run_hash', run_metadata)
```

### Post-Completion Review

After completion, start review with:
```bash
# View executive summary
cat run_logs/{run_hash}/outputs/run_summary.md

# View agentic insights report (recommended)
cat run_logs/{run_hash}/outputs/insights/agentic_insights_report.md

# View structured agent results
cat run_logs/{run_hash}/outputs/insights/agentic_insights.json

# View insights summary (legacy)
cat run_logs/{run_hash}/outputs/reports/insights_summary.md

# View recommendations
cat run_logs/{run_hash}/outputs/reports/recommendations.md

# Check data quality
cat run_logs/{run_hash}/outputs/validation/data_quality_validation.json
```

### Key Output Files

**Agentic Framework Outputs:**
- `outputs/insights/agentic_insights_report.md` - Human-readable insights with business metrics
- `outputs/insights/agentic_insights.json` - Structured agent results
- `outputs/insights/` - Individual agent analysis files

**Business Metrics Included:**
- Duration of analysis (data range)
- App name
- Average daily users
- Average daily new users
- Average D1 retention (true cohort-based)
- Average D3 retention
- Average D7 retention
- Total revenue for given duration
- Average daily revenue
- Total number of whale users

This streamlined workflow ensures **reproducible, traceable, and statistically rigorous analysis** with focus on core product metrics and AI-powered insights for product managers.