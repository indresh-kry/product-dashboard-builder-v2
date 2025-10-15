# Script Inventory

## Active Scripts (Used in Current Runs)

### Phase 0 - System Initialization
| Script | Version | Status | Last Used | Description |
|--------|---------|--------|-----------|-------------|
| `system_health_check.py` | v2.1.0 | ✅ Active | Run 941aad | Enhanced system health validation |

### Phase 1 - Schema Discovery
| Script | Version | Status | Last Used | Description |
|--------|---------|--------|-----------|-------------|
| `schema_discovery_enhanced.py` | v2.0.0 | ✅ Active | Run 941aad | Enhanced schema discovery with type hints |
| `schema_discovery_with_raw_data.py` | v1.0.0 | ✅ Active | Run 941aad | Schema discovery with raw data output |

### Phase 2 - Data Aggregation
| Script | Version | Status | Last Used | Description |
|--------|---------|--------|-----------|-------------|
| `data_aggregation_v3.py` | v3.3.0 | ✅ Active | Run 90f9ac | Final working version with all fixes |
| `data_aggregation_enhanced.py` | v1.2.0 | 🗄️ Legacy | Run 941aad | Enhanced aggregation with table creation |

### Phase 3 - User Segmentation
| Script | Version | Status | Last Used | Description |
|--------|---------|--------|-----------|-------------|
| `user_segmentation_v1.py` | v1.0.0 | ✅ Active | New | Install cohorts, behavioral segments, revenue segments |

### Supporting Scripts
| Script | Version | Status | Last Used | Description |
|--------|---------|--------|-----------|-------------|
| `rules_engine_integration.py` | v1.0.0 | ✅ Active | Run 941aad | Rules engine integration |

## Legacy Scripts (Development History)

### Data Aggregation Development Scripts
| Script | Version | Status | Description |
|--------|---------|--------|-------------|
| `data_aggregation_demo.py` | - | 🗄️ Legacy | Demo version |
| `data_aggregation_final.py` | - | 🗄️ Legacy | Final development version |
| `data_aggregation_final_working.py` | - | 🗄️ Legacy | Working final version |
| `data_aggregation_generator.py` | - | 🗄️ Legacy | Generator version |
| `data_aggregation_generator_fixed.py` | - | 🗄️ Legacy | Fixed generator version |
| `data_aggregation_simple.py` | - | 🗄️ Legacy | Simple version |
| `data_aggregation_simple_working.py` | - | 🗄️ Legacy | Working simple version |
| `data_aggregation_working.py` | - | 🗄️ Legacy | Working version |
| `data_aggregation_working_final.py` | - | 🗄️ Legacy | Working final version |

## Script Usage in Latest Run (941aad)

### Execution Order:
1. **Phase 0**: `system_health_check.py` v2.1.0
2. **Phase 1**: `schema_discovery_with_raw_data.py` v1.0.0
3. **Phase 2**: `data_aggregation_enhanced.py` v1.2.0

### Outputs Generated:
- Schema mapping and analysis files
- Raw data CSV (10,000 rows)
- Aggregated data CSV (1,000 rows)
- SQL generation files

## Future Script Development

### Planned Scripts:
- `phase3_metrics_calculation.py` - Core product metrics calculation
- `phase3_llm_integration.py` - LLM integration for insights
- `phase4_reporting.py` - Report generation
- `phase5_quality_assurance.py` - Quality assurance and validation

### Versioning Guidelines:
- Follow semantic versioning (MAJOR.MINOR.PATCH)
- Include version headers in all scripts
- Document changes in changelog
- Maintain backward compatibility within major versions

## Enhanced Scripts v2.0 (2025-10-14)

### schema_discovery_v3.py
- **Version**: 2.0.0
- **Status**: Active
- **Description**: Enhanced schema discovery with session duration calculation fields and revenue classification
- **Key Features**:
  - Session duration calculation strategy identification
  - Revenue event classification logic
  - Enhanced user identification with data quality assessment
  - Comprehensive data quality reporting
- **Dependencies**: google-cloud-bigquery, pandas, json
- **Environment Variables**: RUN_HASH, DATASET_NAME, APP_FILTER, DATE_START, DATE_END, RAW_DATA_LIMIT

### data_aggregation_enhanced_v2.py
- **Version**: 2.0.0
- **Status**: Active
- **Description**: Enhanced data aggregation with session duration calculation and revenue classification
- **Key Features**:
  - Timestamp-based session duration calculation
  - Revenue type classification (iap, ad, subscription)
  - Dynamic level field generation
  - User cohort analysis with data quality callouts
  - Comprehensive summary reporting
- **Dependencies**: google-cloud-bigquery, pandas, json, python-dotenv
- **Environment Variables**: RUN_HASH, DATASET_NAME, APP_FILTER, DATE_START, DATE_END, AGGREGATION_LIMIT

## Enhanced Scripts v2.0+ (2025-10-14) - Latest Versions

### schema_discovery_v3.py
- **Version**: 2.0.0
- **Status**: ✅ Active
- **Description**: Enhanced schema discovery with session duration calculation fields and revenue classification
- **Key Features**:
  - Session duration calculation strategy identification
  - Revenue event classification logic
  - Enhanced user identification with data quality assessment
  - Comprehensive data quality reporting
- **Dependencies**: google-cloud-bigquery, pandas, json
- **Environment Variables**: RUN_HASH, DATASET_NAME, APP_FILTER, DATE_START, DATE_END, RAW_DATA_LIMIT
- **Last Used**: Run 44046 (Testing)

### data_aggregation_enhanced_v2.py
- **Version**: 2.0.0
- **Status**: ⚠️ Development
- **Description**: Enhanced data aggregation with session duration calculation and revenue classification
- **Key Features**:
  - Timestamp-based session duration calculation
  - Revenue type classification (iap, ad, subscription)
  - Dynamic level field generation
  - User cohort analysis with data quality callouts
  - Comprehensive summary reporting
- **Dependencies**: google-cloud-bigquery, pandas, json, python-dotenv
- **Environment Variables**: RUN_HASH, DATASET_NAME, APP_FILTER, DATE_START, DATE_END, AGGREGATION_LIMIT
- **Issues**: SQL column ambiguity in JOIN operations

### data_aggregation_enhanced_v2_fixed.py
- **Version**: 2.0.1
- **Status**: ⚠️ Development
- **Description**: Fixed version attempting to resolve session_id ambiguity
- **Key Features**: Same as v2.0.0 with table alias fixes
- **Issues**: Still encountering SQL ambiguity issues

### data_aggregation_enhanced_v2_working.py
- **Version**: 2.0.2
- **Status**: ⚠️ Development
- **Description**: Working version with simplified query structure
- **Key Features**: Same as v2.0.0 with simplified GROUP BY
- **Issues**: Still encountering SQL ambiguity issues

### data_aggregation_enhanced_v2_final.py
- **Version**: 2.0.3
- **Status**: ⚠️ Development
- **Description**: Final version with proper table aliases
- **Key Features**: Same as v2.0.0 with comprehensive table aliases
- **Issues**: Still encountering SQL ambiguity issues

### data_aggregation_enhanced_v2_simplified.py
- **Version**: 2.0.4
- **Status**: ⚠️ Development
- **Description**: Simplified working version focusing on core requirements
- **Key Features**: 
  - Session duration calculation (timestamp-based)
  - Revenue classification (iap, ad, subscription)
  - Dynamic level fields
  - Data quality callouts
- **Issues**: SQL column ambiguity in session_id references

## Development Status Summary

### ✅ Successfully Implemented:
- **Schema Discovery v2.0.0**: Fully working with enhanced analysis
- **Session Duration Strategy**: Timestamp-based calculation implemented
- **Revenue Classification**: iap/ad/subscription mapping implemented
- **Data Quality Assessment**: Comprehensive quality scoring and issue identification
- **Dynamic Field Generation**: Level fields based on available events

### ⚠️ In Progress:
- **Data Aggregation Scripts**: Multiple versions created to resolve SQL ambiguity
- **SQL Query Optimization**: Working to resolve column ambiguity in JOIN operations

### 🎯 Next Steps:
- Resolve SQL column ambiguity issue in data aggregation
- Complete final working aggregation script
- Test on larger datasets for cohort analysis
- Implement retention metrics for multi-date analysis

## Script Usage in Latest Test Run (44046)

### Execution Order:
1. **Phase 0**: `system_health_check.py` v2.1.0
2. **Phase 1**: `schema_discovery_v3.py` v3.0.0 ✅
3. **Phase 2**: `data_aggregation_enhanced_v2_simplified.py` v2.0.4 ⚠️

### Outputs Generated:
- Enhanced schema mapping with session analysis
- Revenue classification mapping
- Data quality assessment with user identification issues
- Raw data CSV (10,000 rows)
- SQL query files (multiple versions for debugging)

### Key Findings:
- **Data Quality Issues**: custom_user_id has only 1 unique value
- **Substitute Used**: device_id (2,087 unique values) as primary identifier
- **Session Analysis**: 2,950 unique sessions identified
- **Revenue Events**: 3,181 ad revenue events, 9 IAP events
- **Level Events**: 13 levels available (div_level_1 through div_level_13)

### data_aggregation_v3.py
- **Version**: 2.0.6
- **Status**: ✅ Active
- **Description**: Final working version with all SQL issues resolved
- **Key Features**: 
  - Session duration calculation (timestamp-based)
  - Revenue classification (iap, ad, subscription)
  - Dynamic level fields (13 levels)
  - Data quality callouts
  - Proper GROUP BY handling
  - Session count removed to eliminate ambiguity
- **Dependencies**: google-cloud-bigquery, pandas, json, python-dotenv
- **Environment Variables**: RUN_HASH, DATASET_NAME, APP_FILTER, DATE_START, DATE_END, AGGREGATION_LIMIT
- **Last Used**: Run 44046 (Successfully completed)
- **Output**: 1000 rows exported to CSV successfully

## Final Working Script Status (2025-10-14)

### ✅ Successfully Implemented and Working:
- **Schema Discovery v2.0.0**: Enhanced analysis with session duration strategy and revenue classification
- **Data Aggregation v2.0.6**: Final working version with all critical requirements implemented
- **Session Duration Calculation**: Timestamp-based calculation using MIN/MAX adjusted_timestamp
- **Revenue Classification**: iap/ad/subscription mapping implemented
- **Data Quality Assessment**: Comprehensive quality scoring and issue identification
- **Dynamic Field Generation**: Level fields based on available events (13 levels)
- **Data Quality Callouts**: User identification issues documented and substitutes used

### 🎯 Key Achievements:
1. **Session Duration Calculation**: ✅ Implemented timestamp-based approach
2. **Data Quality Issues**: ✅ Identified custom_user_id issue, using device_id as substitute
3. **Revenue Classification**: ✅ Implemented iap/ad/subscription classification
4. **SQL Ambiguity Resolution**: ✅ Removed session count to eliminate ambiguity
5. **GROUP BY Issues**: ✅ Fixed with proper aggregation functions
6. **Successful Execution**: ✅ 1000 rows exported to CSV

### 📊 Final Test Results (Run 44046):
- **Status**: Success
- **Data Quality Score**: 96.15
- **Primary User ID**: device_id (substitute used)
- **Session Duration**: Implemented with timestamp-based calculation
- **Revenue Classification**: Working (iap, ad, subscription)
- **Level Fields**: 13 levels dynamically generated
- **Output**: 1000 rows successfully exported to CSV

### 🔧 Technical Solutions Applied:
1. **Removed session count** to eliminate session_id ambiguity
2. **Fixed GROUP BY issues** with proper MAX() aggregation for level calculations
3. **Maintained session duration calculations** using CTE approach
4. **Preserved all revenue classification logic**
5. **Kept dynamic level field generation**

## Workflow Orchestration Scripts

### Active Scripts
- **analysis_workflow_orchestrator.py**: v1.0.0
  - **Purpose**: Central orchestrator for entire analysis workflow
  - **Status**: ✅ Active (New implementation)
  - **Features**: 
    - Automated execution of all phases (0-6)
    - Command-line interface with comprehensive options
    - Environment management and run hash generation
    - Error handling and recovery mechanisms
    - Progress tracking and logging
    - Report generation
  - **Usage**: `python scripts/analysis_workflow_orchestrator.py [options]`
  - **Dependencies**: All phase scripts in scripts/ directory
  - **Integration**: Orchestrates all other scripts in the workflow

- **schema_discovery_v3.py**: v3.0.0
  - **Purpose**: Enhanced schema discovery with session duration and revenue classification
  - **Status**: ✅ Active (Renamed from enhanced_v2)
  - **Features**: 
    - Session duration calculation field identification
    - Revenue classification mapping
    - Enhanced user identification with data quality assessment
    - Comprehensive data quality reporting
  - **Usage**: Called by orchestrator in Phase 1
  - **Dependencies**: BigQuery client, pandas, json
  - **Integration**: Provides schema mapping for data aggregation

- **data_aggregation_v3.py**: v3.0.0
  - **Purpose**: Final working data aggregation with all SQL fixes
  - **Status**: ✅ Active (Renamed from enhanced_v2_final_working)
  - **Features**: 
    - Session duration calculation (timestamp-based)
    - Revenue classification (iap/ad/subscription)
    - Dynamic level fields (13 levels)
    - Data quality callouts
    - Proper GROUP BY handling
  - **Usage**: Called by orchestrator in Phase 2
  - **Dependencies**: BigQuery client, pandas, schema mapping
  - **Integration**: Generates user-daily aggregated data

### 📈 Next Steps for Production:
1. Test orchestrator with full workflow execution
2. Test on larger datasets for cohort analysis
3. Implement retention metrics for multi-date analysis
4. Add session count back with proper session_id handling
5. Scale to full dataset processing
