# Script Versioning System

## Version Format
All scripts follow semantic versioning: `MAJOR.MINOR.PATCH`

- **MAJOR**: Breaking changes or complete rewrites
- **MINOR**: New features or significant enhancements
- **PATCH**: Bug fixes or minor improvements

## Current Script Versions

### Phase 0 - System Initialization
- `system_health_check.py` - **v2.1.0** (Enhanced with merged Phase 1 improvements)

### Phase 1 - Schema Discovery
- `schema_discovery_enhanced.py` - **v2.0.0** (Enhanced with type hints and error handling)
- `schema_discovery_with_raw_data.py` - **v1.0.0** (New script with raw data output)

### Phase 2 - Data Aggregation
- `data_aggregation_enhanced.py` - **v1.2.0** (Enhanced with table creation and CSV fallback)

### Supporting Scripts
- `rules_engine_integration.py` - **v1.0.0** (New integration script)

## Version History

### system_health_check.py
- **v1.0.0** - Initial version
- **v2.0.0** - Added dataclass structure and improved validation
- **v2.1.0** - Enhanced with merged Phase 1 improvements

### schema_discovery_enhanced.py
- **v1.0.0** - Initial enhanced version
- **v2.0.0** - Added type hints, improved error handling, project inference

### data_aggregation_enhanced.py
- **v1.0.0** - Initial enhanced version
- **v1.1.0** - Added CSV fallback mechanism
- **v1.2.0** - Enhanced error handling and environment configuration

### schema_discovery_with_raw_data.py
- **v1.0.0** - Initial version with raw data output

### rules_engine_integration.py
- **v1.0.0** - Initial version

## Versioning Rules

1. **Version Header**: Each script must include a version header comment
2. **Changelog**: Document changes in the script header
3. **Backward Compatibility**: Maintain backward compatibility within major versions
4. **Testing**: Test all changes before version bump
5. **Documentation**: Update this file when creating new versions

## Script Header Template

```python
#!/usr/bin/env python3
"""
Script Name - Brief Description
Version: X.Y.Z
Last Updated: YYYY-MM-DD

Changelog:
- vX.Y.Z (YYYY-MM-DD): Description of changes
- vX.Y-1.Z (YYYY-MM-DD): Previous changes

Environment Variables:
- VAR1: Description
- VAR2: Description

Dependencies:
- package1: Description
- package2: Description
"""
```

## Future Script Naming Convention

- `{phase}_{function}_{version}.py` for major versions
- `{phase}_{function}_v{version}.py` for specific versions
- `{phase}_{function}_{feature}.py` for feature-specific scripts

Examples:
- `phase1_schema_discovery_v2.py`
- `phase2_aggregation_enhanced.py`
- `phase3_metrics_calculation.py`

## Version 2.0.0 (2025-10-14)

### Major Enhancements
- **Session Duration Calculation**: Implemented timestamp-based session duration calculation
- **Revenue Classification**: Added revenue type classification (iap, ad, subscription)
- **Data Quality Callouts**: Enhanced reporting of data quality issues and substitutes used
- **Dynamic Field Generation**: Improved level field generation based on available events

### Schema Discovery Enhancements
- Added session analysis with duration calculation strategy
- Enhanced revenue analysis with event classification
- Improved user identification with data quality assessment
- Added comprehensive data quality reporting

### Data Aggregation Enhancements
- Implemented session duration calculation using MIN/MAX timestamps
- Added revenue classification logic with event mapping
- Enhanced user cohort analysis with data quality callouts
- Added comprehensive summary reporting with recommendations

### Breaking Changes
- None (backward compatible)

### Migration Notes
- New scripts are drop-in replacements for v1.x versions
- Enhanced output includes additional analysis fields
- Summary reports provide better visibility into data quality issues

## Version 2.0.4 (2025-10-14) - Latest Development

### Data Aggregation Scripts
- **data_aggregation_enhanced_v2_simplified.py**: v2.0.4
  - Simplified working version focusing on core requirements
  - Session duration calculation (timestamp-based)
  - Revenue classification (iap, ad, subscription)
  - Dynamic level fields
  - Data quality callouts
  - **Status**: Development (SQL ambiguity issue)

### Version 2.0.3 (2025-10-14)

### Data Aggregation Scripts
- **data_aggregation_enhanced_v2_final.py**: v2.0.3
  - Final version with proper table aliases
  - Comprehensive table aliases for all columns
  - **Status**: Development (SQL ambiguity issue)

### Version 2.0.2 (2025-10-14)

### Data Aggregation Scripts
- **data_aggregation_enhanced_v2_working.py**: v2.0.2
  - Working version with simplified query structure
  - Simplified GROUP BY clause
  - **Status**: Development (SQL ambiguity issue)

### Version 2.0.1 (2025-10-14)

### Data Aggregation Scripts
- **data_aggregation_enhanced_v2_fixed.py**: v2.0.1
  - Fixed version attempting to resolve session_id ambiguity
  - Table alias fixes for session_id references
  - **Status**: Development (SQL ambiguity issue)

### Breaking Changes
- None (all versions are development iterations)

### Migration Notes
- Multiple versions created to resolve SQL column ambiguity issues
- All versions maintain the same core functionality
- SQL query structure optimized in each iteration

## Version 2.0.6 (2025-10-14) - Final Working Version

### Data Aggregation Scripts
- **data_aggregation_enhanced_v2_final_working.py**: v2.0.6
  - Final working version with all SQL issues resolved
  - Session duration calculation (timestamp-based)
  - Revenue classification (iap, ad, subscription)
  - Dynamic level fields (13 levels)
  - Data quality callouts
  - Proper GROUP BY handling
  - Session count removed to eliminate ambiguity
  - **Status**: ✅ Active (Successfully tested on Run 44046)
  - **Output**: 1000 rows exported to CSV successfully

### Major Fixes Applied
- **SQL Ambiguity Resolution**: Removed session count to eliminate session_id ambiguity
- **GROUP BY Issues**: Fixed with proper MAX() aggregation for level calculations
- **Session Duration**: Maintained timestamp-based calculation using CTE approach
- **Revenue Classification**: Preserved all iap/ad/subscription logic
- **Dynamic Fields**: Kept level field generation (13 levels)

### Breaking Changes
- Session count removed (can be added back later with proper handling)

### Migration Notes
- This is the final working version for production use
- All critical requirements implemented successfully
- Ready for larger dataset testing and cohort analysis

## Version 1.0.0 (2025-10-14) - Orchestrator Implementation

### Workflow Orchestration
- **analysis_workflow_orchestrator.py**: v1.0.0
  - Central orchestrator for entire analysis workflow
  - Automated execution of all phases (0-6)
  - Command-line interface with comprehensive options
  - Environment management and run hash generation
  - Error handling and recovery mechanisms
  - Progress tracking and logging
  - **Status**: ✅ Active (New implementation)
  - **Features**: Full workflow automation, phase management, reporting

### Orchestrator Features
- **Automated Phase Execution**: Runs all phases in sequence with proper error handling
- **Command-Line Interface**: Comprehensive options for different execution modes
- **Environment Management**: Automatic setup of run-specific environment variables
- **Progress Tracking**: Detailed logging and status reporting
- **Error Recovery**: Configurable error handling and recovery options
- **Report Generation**: Automatic creation of comprehensive run summaries

### Usage Examples
- Full workflow: `python scripts/analysis_workflow_orchestrator.py`
- Schema only: `python scripts/analysis_workflow_orchestrator.py --mode schema-only`
- Quick mode: `python scripts/analysis_workflow_orchestrator.py --quick`
- With filters: `python scripts/analysis_workflow_orchestrator.py --app-filter com.nukebox.mandir --date-start 2025-09-15 --date-end 2025-09-30`
