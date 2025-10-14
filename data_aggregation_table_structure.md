# Data Aggregation Table Structure Proposal (Updated)
## Product Dashboard Builder v2 - User ID x Date Basis

### Overview
This document proposes the structure for our primary data aggregation table that will serve as the foundation for all product metrics and analysis. The table will be organized on a **User ID x Date** basis with comprehensive user journey and engagement tracking.

**Updated based on feedback:**
- Removed ambiguous session start/end times
- Added session duration metrics (count, average, longest)
- Split revenue event counts by type
- Dynamic event field naming based on available data
- Added data quality rules and handling

---

## Core Table Structure

### Primary Key
- **user_id**: Primary user identifier (custom_user_id or device_id fallback)
- **date**: Date of activity (YYYY-MM-DD format)
- **device_id**: Device identifier for cross-device analysis

### User Identification & Cohort Definition
| Field Name | Data Type | Description | Source Logic | Data Quality Rules |
|------------|-----------|-------------|--------------|-------------------|
| `user_id` | STRING | Primary user identifier | custom_user_id (if available) or device_id | Use first non-null value |
| `device_id` | STRING | Device identifier | device_id column | Use first non-null value |
| `user_type` | STRING | User classification | 'new' (first event) or 'returning' | Based on cohort_date |
| `cohort_date` | DATE | First activity date for cohort analysis | MIN(adjusted_timestamp) for user_id | Use earliest valid timestamp |
| `days_since_first_event` | INTEGER | Days since first event (cohort definition) | DATEDIFF(date, cohort_date) | Calculated field |
| `days_since_last_event` | INTEGER | Days since last activity | DATEDIFF(date, MAX(adjusted_timestamp)) | Calculated field |

### Session Metrics (Updated)
| Field Name | Data Type | Description | Source Logic | Data Quality Rules |
|------------|-----------|-------------|--------------|-------------------|
| `total_sessions` | INTEGER | Number of sessions on this date | COUNT(DISTINCT session_id) | Use distinct session identifiers |
| `avg_session_duration_minutes` | FLOAT | Average session duration in minutes | AVG(session_duration) | Exclude null/zero durations |
| `longest_session_duration_minutes` | FLOAT | Longest session duration in minutes | MAX(session_duration) | Exclude null/zero durations |
| `total_session_time_minutes` | FLOAT | Total time spent in sessions | SUM(session_duration) | Sum of all valid session durations |

### Engagement Events & Timestamps (Dynamic)
| Field Name | Data Type | Description | Source Logic | Data Quality Rules |
|------------|-----------|-------------|--------------|-------------------|
| `ftue_complete_time` | TIMESTAMP | First-time user experience completion | MIN(adjusted_timestamp) where event = 'ftue_complete' | **First event only** - ignore duplicates |
| `level_1_time` | TIMESTAMP | First level 1 completion | MIN(adjusted_timestamp) where event = 'div_level_1' | **First event only** - ignore duplicates |
| `level_2_time` | TIMESTAMP | First level 2 completion | MIN(adjusted_timestamp) where event = 'div_level_2' | **First event only** - ignore duplicates |
| `level_3_time` | TIMESTAMP | First level 3 completion | MIN(adjusted_timestamp) where event = 'div_level_3' | **First event only** - ignore duplicates |
| `level_4_time` | TIMESTAMP | First level 4 completion | MIN(adjusted_timestamp) where event = 'div_level_4' | **First event only** - ignore duplicates |
| `level_5_time` | TIMESTAMP | First level 5 completion | MIN(adjusted_timestamp) where event = 'div_level_5' | **First event only** - ignore duplicates |
| `level_6_time` | TIMESTAMP | First level 6 completion | MIN(adjusted_timestamp) where event = 'div_level_6' | **First event only** - ignore duplicates |
| `level_7_time` | TIMESTAMP | First level 7 completion | MIN(adjusted_timestamp) where event = 'div_level_7' | **First event only** - ignore duplicates |
| `max_level_reached` | INTEGER | Highest level reached on this date | MAX(level_number) from level events | Use highest valid level number |

**Note**: Event field names and count will be dynamically determined based on available events in the raw data during schema discovery phase.

### Revenue & Monetization (Updated)
| Field Name | Data Type | Description | Source Logic | Data Quality Rules |
|------------|-----------|-------------|--------------|-------------------|
| `total_revenue` | FLOAT | Total revenue generated | SUM(converted_revenue) where is_revenue_valid = true | Exclude invalid revenue events |
| `total_revenue_usd` | FLOAT | Total revenue in USD | SUM(converted_revenue) where converted_currency = 'USD' | Convert to USD if needed |
| `iap_revenue` | FLOAT | In-app purchase revenue | SUM(converted_revenue) where revenue_type = 'iap' | Use revenue type classification |
| `ad_revenue` | FLOAT | Ad revenue | SUM(converted_revenue) where revenue_type = 'ad' | Use revenue type classification |
| `subscription_revenue` | FLOAT | Subscription revenue | SUM(converted_revenue) where revenue_type = 'subscription' | Use revenue type classification |
| `iap_events_count` | INTEGER | Number of IAP events | COUNT(*) where revenue_type = 'iap' AND is_revenue_valid = true | Count valid IAP events only |
| `ad_events_count` | INTEGER | Number of ad revenue events | COUNT(*) where revenue_type = 'ad' AND is_revenue_valid = true | Count valid ad events only |
| `subscription_events_count` | INTEGER | Number of subscription events | COUNT(*) where revenue_type = 'subscription' AND is_revenue_valid = true | Count valid subscription events only |
| `total_revenue_events_count` | INTEGER | Total number of revenue events | COUNT(*) where is_revenue_valid = true | Sum of all valid revenue events |
| `first_purchase_time` | TIMESTAMP | First purchase timestamp | MIN(adjusted_timestamp) where is_revenue_valid = true | **First event only** - ignore duplicates |
| `last_purchase_time` | TIMESTAMP | Last purchase timestamp | MAX(adjusted_timestamp) where is_revenue_valid = true | Use latest valid timestamp |

### Event Counts & Engagement Metrics
| Field Name | Data Type | Description | Source Logic | Data Quality Rules |
|------------|-----------|-------------|--------------|-------------------|
| `total_events` | INTEGER | Total events on this date | COUNT(*) | Count all events |
| `unique_events` | INTEGER | Number of unique event types | COUNT(DISTINCT name) | Count distinct event names |
| `engagement_score` | FLOAT | Calculated engagement score | Weighted sum of engagement events | Custom scoring algorithm |
| `retention_day_1` | BOOLEAN | Returned on day 1 | EXISTS(activity on date + 1) | Check for any activity next day |
| `retention_day_2` | BOOLEAN | Returned on day 2 | EXISTS(activity on date + 2) | Check for any activity in 2 days |
| `retention_day_3` | BOOLEAN | Returned on day 3 | EXISTS(activity on date + 3) | Check for any activity in 3 days |
| `retention_day_7` | BOOLEAN | Returned on day 7 | EXISTS(activity on date + 7) | Check for any activity in 7 days |
| `retention_day_14` | BOOLEAN | Returned on day 14 | EXISTS(activity on date + 14) | Check for any activity in 14 days |
| `retention_day_30` | BOOLEAN | Returned on day 30 | EXISTS(activity on date + 30) | Check for any activity in 30 days |

### Geographic & Attribution
| Field Name | Data Type | Description | Source Logic | Data Quality Rules |
|------------|-----------|-------------|--------------|-------------------|
| `country` | STRING | User's country | Most frequent country from events | Use mode (most frequent) value |
| `state` | STRING | User's state/province | Most frequent state from events | Use mode (most frequent) value |
| `city` | STRING | User's city | Most frequent city from events | Use mode (most frequent) value |
| `acquisition_channel` | STRING | User acquisition source | Most frequent acquisition source | Use mode (most frequent) value |
| `campaign_id` | STRING | Marketing campaign ID | Most frequent campaign_id | Use mode (most frequent) value |
| `install_date` | DATE | App installation date | MIN(adjusted_timestamp) for user_id | Use earliest valid timestamp |

### Data Quality & Metadata
| Field Name | Data Type | Description | Source Logic | Data Quality Rules |
|------------|-----------|-------------|--------------|-------------------|
| `data_quality_score` | FLOAT | Data quality score (0-100) | Calculated based on null rates and completeness | Custom quality scoring |
| `data_quality_issues` | STRING | List of data quality issues found | JSON array of quality issues | Document all quality problems |
| `last_updated` | TIMESTAMP | When this record was last updated | CURRENT_TIMESTAMP() | System timestamp |
| `run_hash` | STRING | Run identifier for this aggregation | From environment variable | Track data lineage |
| `app_name` | STRING | App name for multi-app datasets | app_longname | Use consistent app naming |

---

## Data Quality Rules & Handling

### 1. **Duplicate Event Handling**
- **Rule**: For milestone events (levels, FTUE, purchases), use **first occurrence only**
- **Implementation**: `MIN(adjusted_timestamp) WHERE event = 'event_name'`
- **Rationale**: Prevents double-counting and ensures accurate progression tracking

### 2. **Session Duration Validation**
- **Rule**: Exclude null, zero, or negative session durations
- **Implementation**: `WHERE session_duration > 0 AND session_duration IS NOT NULL`
- **Fallback**: If no valid sessions, set metrics to 0

### 3. **Revenue Validation**
- **Rule**: Only count revenue events where `is_revenue_valid = true`
- **Implementation**: `WHERE is_revenue_valid = true`
- **Additional**: Validate revenue amounts are positive

### 4. **Geographic Data Consistency**
- **Rule**: Use most frequent (mode) value for geographic fields
- **Implementation**: `MODE(country)`, `MODE(state)`, etc.
- **Fallback**: If no clear mode, use first non-null value

### 5. **Timestamp Validation**
- **Rule**: Ensure timestamps are within reasonable bounds
- **Implementation**: `WHERE adjusted_timestamp BETWEEN '2020-01-01' AND CURRENT_TIMESTAMP()`
- **Fallback**: Exclude invalid timestamps from calculations

### 6. **User ID Consistency**
- **Rule**: Use custom_user_id if available, fallback to device_id
- **Implementation**: `COALESCE(custom_user_id, device_id)`
- **Validation**: Ensure user_id is never null

---

## Dynamic Field Generation

### Event Field Discovery
```sql
-- Discover available events during schema discovery
SELECT DISTINCT name as event_name
FROM raw_data
WHERE name LIKE 'div_level_%' OR name LIKE '%level%' OR name = 'ftue_complete'
ORDER BY name;
```

### Dynamic Field Creation
Based on discovered events, the aggregation script will:
1. **Identify level events**: `div_level_1`, `div_level_2`, etc.
2. **Create corresponding timestamp fields**: `level_1_time`, `level_2_time`, etc.
3. **Create level count fields**: `level_1_count`, `level_2_count`, etc.
4. **Update max_level_reached logic**: Based on available levels

### Revenue Type Classification
```sql
-- Classify revenue events by type
CASE 
    WHEN event_name LIKE '%iap%' OR event_name LIKE '%purchase%' THEN 'iap'
    WHEN event_name LIKE '%ad%' OR event_name LIKE '%admon%' THEN 'ad'
    WHEN event_name LIKE '%subscription%' OR event_name LIKE '%sub%' THEN 'subscription'
    ELSE 'other'
END as revenue_type
```

---

## Sample Data Structure (Updated)

```sql
-- Example of what the aggregated table would look like
SELECT 
    user_id,
    device_id,
    date,
    user_type,
    cohort_date,
    days_since_first_event,
    days_since_last_event,
    -- Session metrics (updated)
    total_sessions,
    avg_session_duration_minutes,
    longest_session_duration_minutes,
    total_session_time_minutes,
    -- Engagement events (dynamic based on available data)
    ftue_complete_time,
    level_1_time,
    level_2_time,
    level_3_time,
    max_level_reached,
    -- Revenue metrics (updated)
    total_revenue,
    total_revenue_usd,
    iap_revenue,
    ad_revenue,
    subscription_revenue,
    iap_events_count,
    ad_events_count,
    subscription_events_count,
    total_revenue_events_count,
    first_purchase_time,
    -- Event counts
    total_events,
    unique_events,
    engagement_score,
    -- Retention flags
    retention_day_1,
    retention_day_2,
    retention_day_7,
    -- Geographic & attribution
    country,
    state,
    acquisition_channel,
    install_date,
    -- Data quality
    data_quality_score,
    data_quality_issues,
    last_updated,
    run_hash,
    app_name
FROM user_daily_aggregation
WHERE date BETWEEN '2025-09-01' AND '2025-09-07'
  AND app_name = 'com.nukebox.mandir'
ORDER BY user_id, date;
```

---

## Updated Benefits

### 1. **Data Quality Assurance**
- Comprehensive data quality rules
- Duplicate event handling
- Validation for all critical fields
- Quality scoring and issue tracking

### 2. **Flexible Event Handling**
- Dynamic field generation based on available data
- Adaptable to different app event structures
- Extensible for new event types

### 3. **Improved Session Metrics**
- Removed ambiguous session timestamps
- Added meaningful session duration metrics
- Better session quality validation

### 4. **Enhanced Revenue Tracking**
- Split revenue event counts by type
- Better revenue classification
- Improved revenue validation

### 5. **Robust Error Handling**
- Clear data quality rules
- Fallback mechanisms for missing data
- Comprehensive validation logic

---

## Implementation Notes

### 1. **Schema Discovery Integration**
- Use schema discovery results to determine available events
- Generate dynamic field names based on discovered events
- Create appropriate validation rules for each event type

### 2. **Data Quality Monitoring**
- Track data quality scores for each aggregation run
- Log data quality issues for investigation
- Provide quality metrics in final outputs

### 3. **Performance Optimization**
- Use appropriate indexes on user_id, date, and device_id
- Partition by date for optimal query performance
- Consider materialized views for complex calculations

This updated structure provides a robust, data-quality-aware foundation for all product metrics while maintaining flexibility for different data sources and event structures.
