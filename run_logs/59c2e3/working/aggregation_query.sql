
    -- Enhanced User Daily Aggregation with Session Duration and Revenue Classification (Final Working)
    -- Generated for run: 59c2e3
    -- Data Quality Issues: None
    
    WITH session_durations AS (
        SELECT 
            session_id,
            DATE(adjusted_timestamp) as date,
            MIN(adjusted_timestamp) as session_start,
            MAX(adjusted_timestamp) as session_end,
            TIMESTAMP_DIFF(MAX(adjusted_timestamp), MIN(adjusted_timestamp), MINUTE) as session_duration_minutes
        FROM `gc-prod-459709.nbs_dataset.singular_user_level_event_data`
        WHERE app_longname = 'com.nukebox.mandir' AND DATE(adjusted_timestamp) BETWEEN '2025-09-08' AND '2025-09-30'
        AND session_id IS NOT NULL
        GROUP BY session_id, DATE(adjusted_timestamp)
    ),
    
    user_cohorts AS (
        SELECT 
            COALESCE(device_id, device_id) as user_id,
            MIN(DATE(adjusted_timestamp)) as cohort_date
        FROM `gc-prod-459709.nbs_dataset.singular_user_level_event_data`
        WHERE app_longname = 'com.nukebox.mandir' AND DATE(adjusted_timestamp) BETWEEN '2025-09-08' AND '2025-09-30'
        GROUP BY COALESCE(device_id, device_id)
    )
    
    SELECT 
        -- Primary Key
        COALESCE(device_id, device_id) as user_id,
        device_id,
        DATE(t.adjusted_timestamp) as date,
        
        -- User Cohort Information
        uc.cohort_date,
        DATE_DIFF(ANY_VALUE(DATE(t.adjusted_timestamp)), uc.cohort_date, DAY) as days_since_first_event,
        CASE 
            WHEN DATE_DIFF(ANY_VALUE(DATE(t.adjusted_timestamp)), uc.cohort_date, DAY) = 0 THEN 'new'
            ELSE 'returning'
        END as user_type,
        
        -- Session Duration Metrics (Enhanced with Duration Calculation)
        -- Note: Session count removed to eliminate SQL ambiguity
        AVG(sd.session_duration_minutes) as avg_session_duration_minutes,
        MAX(sd.session_duration_minutes) as longest_session_duration_minutes,
        SUM(sd.session_duration_minutes) as total_session_time_minutes,
        
        -- Revenue Metrics (Enhanced with Classification)
        SUM(CASE WHEN is_revenue_valid = true THEN converted_revenue ELSE 0 END) as total_revenue,
        SUM(CASE WHEN is_revenue_valid = true AND converted_currency = 'USD' THEN converted_revenue ELSE 0 END) as total_revenue_usd,
        
        -- Revenue by Type (Enhanced Generic Classification)
        SUM(CASE WHEN is_revenue_valid = true AND (
            UPPER(name) LIKE '%IAP%' OR 
            UPPER(name) LIKE '%PURCHASE%' OR 
            UPPER(name) LIKE '%BUY%' OR
            UPPER(name) LIKE '%INAPP%' OR
            UPPER(name) LIKE '%TRANSACTION%'
        ) THEN converted_revenue ELSE 0 END) as iap_revenue,
        
        SUM(CASE WHEN is_revenue_valid = true AND (
            UPPER(name) LIKE '%AD%' OR 
            UPPER(name) LIKE '%ADS%' OR 
            UPPER(name) LIKE '%ADMON%' OR
            UPPER(name) LIKE '%ADVERTISEMENT%' OR
            UPPER(name) LIKE '%BANNER%' OR
            UPPER(name) LIKE '%INTERSTITIAL%' OR
            UPPER(name) LIKE '%REWARDED%'
        ) THEN converted_revenue ELSE 0 END) as ad_revenue,
        
        SUM(CASE WHEN is_revenue_valid = true AND (
            name LIKE '%sub%' OR 
            name LIKE '%subscription%' OR 
            name LIKE '%recurring%' OR
            name LIKE '%premium%' OR
            name LIKE '%pro%'
        ) THEN converted_revenue ELSE 0 END) as subscription_revenue,
        
        -- Revenue Event Counts by Type (Enhanced Generic Classification)
        COUNT(CASE WHEN is_revenue_valid = true AND (
            UPPER(name) LIKE '%IAP%' OR 
            UPPER(name) LIKE '%PURCHASE%' OR 
            UPPER(name) LIKE '%BUY%' OR
            UPPER(name) LIKE '%INAPP%' OR
            UPPER(name) LIKE '%TRANSACTION%'
        ) THEN 1 END) as iap_events_count,
        
        COUNT(CASE WHEN is_revenue_valid = true AND (
            UPPER(name) LIKE '%AD%' OR 
            UPPER(name) LIKE '%ADS%' OR 
            UPPER(name) LIKE '%ADMON%' OR
            UPPER(name) LIKE '%ADVERTISEMENT%' OR
            UPPER(name) LIKE '%BANNER%' OR
            UPPER(name) LIKE '%INTERSTITIAL%' OR
            UPPER(name) LIKE '%REWARDED%'
        ) THEN 1 END) as ad_events_count,
        
        COUNT(CASE WHEN is_revenue_valid = true AND (
            name LIKE '%sub%' OR 
            name LIKE '%subscription%' OR 
            name LIKE '%recurring%' OR
            name LIKE '%premium%' OR
            name LIKE '%pro%'
        ) THEN 1 END) as subscription_events_count,
        COUNT(CASE WHEN is_revenue_valid = true THEN 1 END) as total_revenue_events_count,
        
        -- Revenue Timestamps
        MIN(CASE WHEN is_revenue_valid = true THEN adjusted_timestamp END) as first_purchase_time,
        MAX(CASE WHEN is_revenue_valid = true THEN adjusted_timestamp END) as last_purchase_time,
        
        -- Event Counts & Engagement Metrics
        COUNT(*) as total_events,
        COUNT(DISTINCT name) as unique_events,
        
        -- Key Milestone Events
        MIN(CASE WHEN name = 'ftue_complete' THEN adjusted_timestamp END) as ftue_complete_time,
        MIN(CASE WHEN name = 'game_complete' THEN adjusted_timestamp END) as game_complete_time,
        
        -- Dynamic Level Fields
        MIN(CASE WHEN name = 'div_level_1' THEN adjusted_timestamp END) as level_1_time, MIN(CASE WHEN name = 'div_level_2' THEN adjusted_timestamp END) as level_2_time, MIN(CASE WHEN name = 'div_level_3' THEN adjusted_timestamp END) as level_3_time, MIN(CASE WHEN name = 'div_level_4' THEN adjusted_timestamp END) as level_4_time, MIN(CASE WHEN name = 'div_level_5' THEN adjusted_timestamp END) as level_5_time, MIN(CASE WHEN name = 'div_level_6' THEN adjusted_timestamp END) as level_6_time, MIN(CASE WHEN name = 'div_level_7' THEN adjusted_timestamp END) as level_7_time, MIN(CASE WHEN name = 'div_level_8' THEN adjusted_timestamp END) as level_8_time, MIN(CASE WHEN name = 'div_level_9' THEN adjusted_timestamp END) as level_9_time,
        
        -- Level Counts
        COUNT(CASE WHEN name = 'div_level_1' THEN 1 END) as level_1_count, COUNT(CASE WHEN name = 'div_level_2' THEN 1 END) as level_2_count, COUNT(CASE WHEN name = 'div_level_3' THEN 1 END) as level_3_count, COUNT(CASE WHEN name = 'div_level_4' THEN 1 END) as level_4_count, COUNT(CASE WHEN name = 'div_level_5' THEN 1 END) as level_5_count, COUNT(CASE WHEN name = 'div_level_6' THEN 1 END) as level_6_count, COUNT(CASE WHEN name = 'div_level_7' THEN 1 END) as level_7_count, COUNT(CASE WHEN name = 'div_level_8' THEN 1 END) as level_8_count, COUNT(CASE WHEN name = 'div_level_9' THEN 1 END) as level_9_count,
        
        
        -- Max Level Reached (Fixed with proper aggregation)
        MAX(CASE 
            WHEN name = 'div_level_1' THEN 1
WHEN name = 'div_level_2' THEN 2
WHEN name = 'div_level_3' THEN 3
WHEN name = 'div_level_4' THEN 4
WHEN name = 'div_level_5' THEN 5
WHEN name = 'div_level_6' THEN 6
WHEN name = 'div_level_7' THEN 7
WHEN name = 'div_level_8' THEN 8
WHEN name = 'div_level_9' THEN 9
            ELSE 0
        END) as max_level_reached,
        
        -- Geographic & Attribution
        ANY_VALUE(country) as country,
        ANY_VALUE(state) as state,
        ANY_VALUE(city) as city,
        ANY_VALUE(install_source) as acquisition_channel,
        ANY_VALUE(campaign_id) as campaign_id,
        ANY_VALUE(campaign_name) as campaign_name,
        ANY_VALUE(utm_source) as utm_source,
        ANY_VALUE(utm_campaign) as utm_campaign,
        
        -- Data Quality & Metadata
        96.15 as data_quality_score,
        CURRENT_TIMESTAMP() as last_updated,
        '59c2e3' as run_hash,
        ANY_VALUE(app_longname) as app_name,
        
        -- Data Quality Issues (JSON)
        '[]' as data_quality_issues
        
    FROM `gc-prod-459709.nbs_dataset.singular_user_level_event_data` t
    LEFT JOIN session_durations sd ON t.session_id = sd.session_id AND DATE(t.adjusted_timestamp) = sd.date
    LEFT JOIN user_cohorts uc ON COALESCE(device_id, device_id) = uc.user_id
    WHERE app_longname = 'com.nukebox.mandir' AND DATE(adjusted_timestamp) BETWEEN '2025-09-08' AND '2025-09-30'
    GROUP BY 
        COALESCE(device_id, device_id),
        device_id,
        DATE(t.adjusted_timestamp),
        uc.cohort_date
    HAVING DATE(ANY_VALUE(t.adjusted_timestamp)) BETWEEN '2025-09-15' AND '2025-09-30'
    ORDER BY 
        COALESCE(device_id, device_id),
        DATE(t.adjusted_timestamp)
    LIMIT 100000;
    