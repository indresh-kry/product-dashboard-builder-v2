
## AppsFlyer adaptation changes (branch: appsflyer-test)

- Config mappings applied for AppsFlyer schema:
  - TIMESTAMP_COLUMN: event_time
  - EVENT_NAME_COLUMN: event_name
  - USER_ID_COLUMN: ip (primary)
  - REVENUE_COLUMN: event_revenue_usd
  - REVENUE_VALIDATION: inferred as event_revenue_usd > 0
  - COUNTRY_COLUMN: country_code

- Edits in `scripts/data_aggregation_v3.py`:
  - Parameterized timestamp, event name, revenue, and country columns via environment variables.
  - Replaced references to adjusted_timestamp/name/converted_revenue/is_revenue_valid with mapped columns and revenue > 0 logic.
  - Primary user id now prefers env USER_ID_COLUMN; falls back to schema recommendations.
  - WHERE date filter uses configured TIMESTAMP_COLUMN.
  - Maintains session metrics join; compatible with timestamp-based heuristic output from discovery.

- Edits in `scripts/schema_discovery_v3.py`:
  - WHERE date filter and ORDER BY use TIMESTAMP_COLUMN from env.
  - Event taxonomy uses EVENT_NAME_COLUMN from env.
  - Primary user id in recommendations overridden by env USER_ID_COLUMN when provided.
  - Ensures sampled raw data is saved (set RAW_DATA_LIMIT to 500 in run).

- Run settings for AF dataset:
  - DATASET_NAME: gc-prod-459709.fm_ingest.highway_racer_v1
  - Filters: ALL_APPS, 2025-08-01 → 2025-08-05
  - RAW_DATA_LIMIT: 500
  - AGGREGATION_LIMIT: 10000
  - TARGET_DATASET: analysis_results (for safety validator)

- Frontend: skipped shadcn-ui install for this repo.

### Execution/debug notes captured

- Disabled table creation during aggregation via `SKIP_TABLE_CREATION=true`; CSV export used instead.
- Installed `db-dtypes` to enable BigQuery `to_dataframe()` for CSV export.
- Segmentation phase dependencies (e.g., `scipy`) not installed; phase intentionally out of scope for this AF run.
- Agentic insights attempted but OpenAI key load is intentionally ignored for this scope.
- Verified 500-row raw sample is written to `run_logs/{run_hash}/outputs/raw_data/sampled_raw_data.csv` on each run.

### [2025-10-30] Schema & Aggregation Enrichment for AF Data Source

- Primary user identifier set to appsflyer_id (was ip).
- Revenue event type is now classified by event_name where event_revenue_usd is not null (af_purchase = IAP).
- Session duration calculated as the delta between first and last event per user-day, capped at 30 mins.
- Funnel step mapping/imputation based on event_name (including bossladder levels).
- Campaign/attribution uses install_source, media_source, and channel fields.
- Null fields (100% null) are ignored/skipped in mapping.
- Advanced monetization fields are ignored for this mapping.
- Full details: See code changes in schema_discovery_v3.py and data_aggregation_v3.py.

- Hotfix: All SQL and python aggregation logic now uses {timestamp_col} parameter for timestamp fields (e.g., event_time), no hardcoded 'adjusted_timestamp' remains for AF pipeline.
- Hotfix: Purged all SQL string template references to 'adjusted_timestamp'; now only uses TIMESTAMP_COLUMN/event_time everywhere in pipeline scripts for full dataset compatibility.
- Brute-force patch: All hardcoded and string template references to 'adjusted_timestamp' eliminated from schema discovery, aggregation, and orchestrator. All pipeline time logic now parameterized (event_time for AF).
- Brute-force patch: All custom_user_id field references purged. User-level mapping, session, and aggregation logic is now parameterized and uses appsflyer_id, with fallback to customer_user_id if specified.
- Brute-force patch: All references and SQL templates/fallbacks involving 'converted_revenue' are now purged; all revenue calculations/filters use revenue_col, which is set to event_revenue_usd for AF workflows.
- Brute-force patch: All legacy and SQL template uses of the 'name' field purged from schema/aggregation/funnel logic. Everything fully parameterized using event_name_col (event_name for AF data).
- Patch: All install_source references purged and replaced by available attribution columns media_source, channel, partner per AF data. No aggregation/group-by uses install_source for AF runs.

- Config update: Adopted reusable field_mapping.json storing all column mappings (user_id, event_name, revenue, timestamp, attribution, etc.) for each run. All pipeline scripts now load and use this field mapping, so adapting to new datasets requires only a JSON change, not code edits.

### [2025-10-31] Fixes for Cohort Retention Analysis and Missing Files

**Issue 1: Missing Files**
- Fixed: Added `behavioral_segments_daily.csv` generation in `user_segmentation_v1.py` to save behavioral segment data (churned, high_engagement, moderate_engagement, low_engagement) at user-daily level.
- Fixed: Added `retention_by_cohort_date.csv` generation in `user_segmentation_v1.py` to calculate and save retention rates (D1, D3, D7, D14, D30) by cohort date. This file was expected by quality validation but was not being generated.

**Issue 2: Cohort Retention Analysis Limited Dataset**
- Fixed: Updated `cohort_retention_loader.py` (v2.1.0) to load ALL retention-related cohort files:
  - `retention_by_cohort_date.csv` (primary)
  - `revenue_by_cohort_date.csv`
  - `dau_by_cohort_date.csv`
  - `engagement_by_cohort_date.csv`
  - `funnel_by_cohort_date.csv`
- Fixed: Updated `cohort_retention_generator.py` (v2.1.0) to send the ENTIRE filtered dataset to the LLM (not just first 5 rows), similar to how geographic analysis was fixed. This ensures comprehensive cohort retention analysis with all available data.

### [2025-10-31] Event Funnel Conversion Rate Calculation Fix

**Issue: Conversion rates calculated incorrectly**
- Fixed: Updated `visualization_generator.py` in `create_event_funnel_chart()` method to calculate all conversion rates relative to "App Opened" (funnel_data[1]) instead of the previous step (funnel_data[i-1]). This ensures consistent conversion rate calculations where all steps are measured as a percentage of users who opened the app, rather than step-by-step conversion rates.

### [2025-10-31] Full Dataset to LLM for All Prompt Generators

**Issue: Some prompt generators were only sending first 5 rows (.head()) instead of full dataset**
- Fixed: Updated all prompt generators that were using the base class `.head()` method to override `format_data_for_prompt()` and send the ENTIRE dataset using `.to_string(index=False)`:
  - `daily_metrics_generator.py` (v2.1.0) - Now sends full daily metrics dataset
  - `user_segmentation_generator.py` (v2.1.0) - Now sends full user segmentation dataset
  - `revenue_optimization_generator.py` (v2.1.0) - Now sends full revenue optimization dataset
  - `data_quality_generator.py` (v2.1.0) - Now sends full data quality dataset
- Already updated:
  - `cohort_retention_generator.py` (v2.1.0) - Already sends full dataset
  - `geographic_generator.py` (v2.1.0) - Already sends full dataset
- This ensures comprehensive analysis with all available data for all agent types, not just a sample of 5 rows.

### [2025-10-31] Fixes for Run 3718db Issues

**Issue 1: LLM Context Overflow for Daily Metrics**
- Problem: Daily metrics dataset was too large (416646 tokens), exceeding LLM context limit (128000 tokens)
- Fixed: Updated `daily_metrics_loader.py` (v2.1.0) to implement data size limits:
  - Maximum 500 rows sent to LLM (est. 200 tokens/row = ~100k tokens)
  - For datasets > 500 rows, takes first 250 and last 250 rows (sorted by date) to preserve trends
  - Applies same limit to all daily metric files
  - Includes note in summary about data summarization
- This prevents context overflow while preserving key trends from beginning and end of date range

**Issue 2: level_1_time is NULL Despite car_bought Event in field_mapping.json**
- Problem: Level 1 completion time was NULL even though `car_bought` is specified in `level_1_completion_events`
- Fixed: Updated `data_aggregation_v3.py` in `generate_level_fields()`:
  - Changed SQL condition from `LOWER({event_name_col}) = '{event.lower()}'` to `TRIM(LOWER({event_name_col})) = '{event_clean}'`
  - Added TRIM to handle any whitespace in event names
  - Added debug logging to print Level 1 SQL condition and events from field_mapping for troubleshooting
  - Improved event name matching to handle edge cases with whitespace
- This ensures `car_bought` events are correctly identified and `level_1_time` is populated

### [2025-10-31] LLM Context Handling for All Data Loaders

**Issue: Risk of LLM context overflow for all agent types**
- Fixed: Implemented data size limits across ALL data loaders to prevent LLM context overflow:
  - `daily_metrics_loader.py` (v2.1.0) - Already updated, max 500 rows
  - `revenue_optimization_loader.py` (v2.1.0) - Max 500 rows, takes first/last portions for time-series data
  - `cohort_retention_loader.py` (v2.2.0) - Max 500 rows per file, preserves cohort trends (first/last cohorts)
  - `geographic_loader.py` (v2.2.0) - Max 500 rows after geographic filtering, prioritizes high-revenue countries
  - `user_segmentation_loader.py` (v2.1.0) - Max 100 KB JSON, max 500 rows for CSV files
  - `data_quality_loader.py` (v2.1.0) - Max 100 KB JSON with size monitoring
- All loaders now:
  - Limit data to ~500 rows or 100KB (est. ~100k tokens total)
  - Preserve trends for time-series data (first/last portions)
  - Add logging about data summarization
  - Include notes in summary about data being summarized
- This prevents LLM context overflow errors (128k token limit) while preserving key analytical insights

### [2025-10-31] New Visualizations Added to Report

**Added visualizations to enhance reporting:**
- Updated `visualization_generator.py` (v1.2.0) with new visualizations:
  1. **User Engagement Trends** - Added third subplot:
     - Daily user count by acquisition channel (using `media_source` from aggregate table)
     - Shows top 10 channels by total user count
     - Multi-line chart with separate lines per channel
  2. **Revenue Performance** - Expanded to 4 subplots:
     - Existing: Daily Revenue Trend and Revenue by Type Trend
     - New: Daily total revenue by acquisition channel (using `media_source` from aggregate table)
     - New: Daily total revenue by geography (using `country` from aggregate table)
     - Both new charts show top 10 channels/countries by total revenue
  3. **ARPU Performance** - New section before Retention Charts:
     - Day-wise overall average revenue per user (ARPU) chart
       - Calculated as: total revenue per day / unique users per day
     - Day-wise ARPU chart by acquisition channel (using `media_source` from aggregate table)
       - Shows top 10 channels by total revenue
       - ARPU calculated per channel per day
- All new visualizations:
  - Use data from aggregated_data.csv (aggregate table)
  - Respect date filtering (last complete date)
  - Show top 10 items to avoid clutter
  - Include proper legends and labels
  - Handle missing data gracefully


