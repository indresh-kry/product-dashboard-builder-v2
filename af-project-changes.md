
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


