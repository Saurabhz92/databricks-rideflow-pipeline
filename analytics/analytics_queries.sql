-- =============================================================================
--  RideFlow Analytics Platform — Sample SQL Analytics Queries
--  Run against Gold Delta tables in Databricks SQL / Tableau / Power BI
-- =============================================================================


-- ===========================================================================
-- 1. REAL-TIME DASHBOARD: Rides per City in the Last 15 Minutes
-- ===========================================================================

SELECT
    city,
    SUM(total_rides)          AS rides_last_15min,
    SUM(completed_rides)      AS completed_rides,
    ROUND(AVG(avg_fare), 2)   AS avg_fare_usd,
    ROUND(SUM(total_gross_revenue), 2) AS gross_revenue_usd,
    ROUND(AVG(avg_surge_multiplier), 2) AS avg_surge,
    ROUND(AVG(completion_rate), 2)      AS completion_rate_pct
FROM rideflow_gold.city_ride_metrics
WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 15 MINUTES
GROUP BY city
ORDER BY rides_last_15min DESC;


-- ===========================================================================
-- 2. SURGE PRICING ALERT: Cities with Active Surge Triggers
-- ===========================================================================

SELECT
    city,
    window_start,
    ride_requests,
    drivers_active,
    demand_supply_ratio,
    recommended_surge_multiplier,
    surge_tier,
    CASE
        WHEN surge_tier = 'EXTREME' THEN '🔴 EXTREME'
        WHEN surge_tier = 'HIGH'    THEN '🟠 HIGH'
        WHEN surge_tier = 'MODERATE' THEN '🟡 MODERATE'
        ELSE '🟢 NORMAL'
    END AS surge_status
FROM rideflow_gold.surge_pricing
WHERE surge_triggered = TRUE
  AND window_start >= CURRENT_TIMESTAMP() - INTERVAL 10 MINUTES
ORDER BY demand_supply_ratio DESC;


-- ===========================================================================
-- 3. DRIVER PERFORMANCE LEADERBOARD: Top 20 Drivers by Efficiency Score
-- ===========================================================================

SELECT
    driver_id,
    city,
    ROUND(AVG(efficiency_score), 2)       AS avg_efficiency_score,
    SUM(trips_completed)                   AS total_completed_trips,
    ROUND(SUM(total_earnings), 2)          AS total_earnings_usd,
    ROUND(AVG(utilization_rate), 2)        AS avg_utilization_pct,
    ROUND(AVG(avg_earnings_per_trip), 2)   AS avg_earnings_per_trip
FROM rideflow_gold.driver_utilization
WHERE window_start_date = CURRENT_DATE()
GROUP BY driver_id, city
HAVING SUM(trips_completed) > 0
ORDER BY avg_efficiency_score DESC
LIMIT 20;


-- ===========================================================================
-- 4. HOURLY REVENUE TREND: Last 24 Hours by City
-- ===========================================================================

SELECT
    city,
    DATE_TRUNC('hour', window_start)       AS hour_bucket,
    SUM(total_rides)                        AS total_rides,
    SUM(completed_rides)                    AS completed_rides,
    ROUND(SUM(total_gross_revenue), 2)      AS gross_revenue_usd,
    ROUND(SUM(total_net_revenue), 2)        AS net_revenue_usd,
    ROUND(AVG(avg_fare), 2)                 AS avg_fare_usd,
    ROUND(AVG(avg_surge_multiplier), 2)     AS avg_surge_multiplier
FROM rideflow_gold.city_ride_metrics
WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY city, DATE_TRUNC('hour', window_start)
ORDER BY city, hour_bucket;


-- ===========================================================================
-- 5. RIDE FUNNEL CONVERSION ANALYSIS: Today's Conversion Rates
-- ===========================================================================

SELECT
    city,
    SUM(stage_1_requested)      AS total_requested,
    SUM(stage_2_assigned)       AS total_assigned,
    SUM(stage_3_started)        AS total_started,
    SUM(stage_4_completed)      AS total_completed,
    SUM(stage_5_payment)        AS total_payments,
    ROUND(AVG(assignment_rate), 2)  AS avg_assignment_rate_pct,
    ROUND(AVG(completion_rate), 2)  AS avg_completion_rate_pct,
    ROUND(AVG(payment_rate), 2)     AS avg_payment_rate_pct,
    -- Overall funnel: requested → payment
    ROUND(
        SUM(stage_5_payment) / GREATEST(SUM(stage_1_requested), 1) * 100, 2
    )                           AS end_to_end_conversion_pct
FROM rideflow_gold.funnel_analysis
WHERE event_date = CURRENT_DATE()
GROUP BY city
ORDER BY end_to_end_conversion_pct DESC;


-- ===========================================================================
-- 6. DEMAND vs SUPPLY GAP: Cities with Unmet Demand
-- ===========================================================================

SELECT
    city,
    MAX(window_start)          AS latest_window,
    AVG(ride_requests)         AS avg_ride_requests,
    AVG(drivers_active)        AS avg_drivers_active,
    AVG(demand_supply_ratio)   AS avg_demand_supply_ratio,
    -- Unmet rides = rides without a driver
    ROUND(AVG(ride_requests - drivers_active), 0) AS avg_unmet_demand,
    COUNT_IF(surge_triggered)  AS surge_trigger_count,
    -- Most frequent surge tier in last hour
    MODE(surge_tier)           AS dominant_surge_tier
FROM rideflow_gold.surge_pricing
WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
GROUP BY city
ORDER BY avg_demand_supply_ratio DESC;


-- ===========================================================================
-- 7. TIME TRAVEL: Compare Today's Revenue vs 7 Days Ago
-- ===========================================================================

-- Current week
WITH current_week AS (
    SELECT city, SUM(total_gross_revenue) AS revenue_this_week
    FROM rideflow_gold.city_ride_metrics
    WHERE window_start_date BETWEEN DATE_SUB(CURRENT_DATE(), 7) AND CURRENT_DATE()
    GROUP BY city
),
-- Prior week (via Delta Time Travel)
prior_week AS (
    SELECT city, SUM(total_gross_revenue) AS revenue_prior_week
    FROM rideflow_gold.city_ride_metrics TIMESTAMP AS OF DATE_SUB(CURRENT_DATE(), 7)
    WHERE window_start_date BETWEEN DATE_SUB(CURRENT_DATE(), 14) AND DATE_SUB(CURRENT_DATE(), 7)
    GROUP BY city
)
SELECT
    c.city,
    ROUND(c.revenue_this_week, 2)           AS revenue_this_week_usd,
    ROUND(p.revenue_prior_week, 2)          AS revenue_prior_week_usd,
    ROUND(c.revenue_this_week - p.revenue_prior_week, 2) AS revenue_delta_usd,
    ROUND(
        (c.revenue_this_week - p.revenue_prior_week)
        / GREATEST(p.revenue_prior_week, 1) * 100, 2
    )                                       AS revenue_growth_pct
FROM current_week c
LEFT JOIN prior_week p ON c.city = p.city
ORDER BY revenue_growth_pct DESC;


-- ===========================================================================
-- 8. DATA QUALITY MONITORING: Dead-Letter Rate by Day
-- ===========================================================================

SELECT
    event_date,
    COUNT(*) AS dead_letter_count,
    COUNT_IF(dead_letter_reason = 'NULL_RIDE_ID')     AS null_ride_id_count,
    COUNT_IF(dead_letter_reason = 'NULL_EVENT_TYPE')  AS null_event_type_count,
    COUNT_IF(dead_letter_reason = 'SCHEMA_MISMATCH')  AS schema_mismatch_count
FROM rideflow_bronze.ride_events_dead_letter
WHERE event_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY event_date
ORDER BY event_date DESC;


-- ===========================================================================
-- 9. PEAK HOUR ANALYSIS: Best and Worst Hours per City
-- ===========================================================================

WITH hourly_stats AS (
    SELECT
        city,
        HOUR(window_start)              AS hour_of_day,
        ROUND(AVG(total_rides), 1)      AS avg_rides_per_hour,
        ROUND(AVG(avg_fare), 2)         AS avg_fare,
        ROUND(AVG(avg_surge_multiplier), 2) AS avg_surge,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY AVG(total_rides) DESC) AS peak_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY AVG(total_rides) ASC)  AS off_peak_rank
    FROM rideflow_gold.city_ride_metrics
    WHERE window_start_date >= DATE_SUB(CURRENT_DATE(), 30)
    GROUP BY city, HOUR(window_start)
)
SELECT
    city,
    MAX(CASE WHEN peak_rank = 1     THEN hour_of_day END) AS peak_hour,
    MAX(CASE WHEN peak_rank = 1     THEN avg_rides_per_hour END) AS peak_rides_per_hr,
    MAX(CASE WHEN off_peak_rank = 1 THEN hour_of_day END) AS off_peak_hour,
    MAX(CASE WHEN off_peak_rank = 1 THEN avg_rides_per_hour END) AS off_peak_rides_per_hr
FROM hourly_stats
GROUP BY city
ORDER BY peak_rides_per_hr DESC;


-- ===========================================================================
-- 10. DELTA LAKE: ACID MERGE — Reprocess & Correct Fare for a Specific City
-- ===========================================================================

MERGE INTO rideflow_silver.ride_events_clean AS target
USING (
    -- Corrected records (e.g., from a reprocessing job)
    SELECT ride_id, event_type, 
           fare_amount * 1.10 AS corrected_fare,   -- Apply 10% fare correction
           CURRENT_TIMESTAMP() AS silver_processed_ts
    FROM rideflow_silver.ride_events_clean
    WHERE city = 'New York'
      AND event_date = '2024-01-15'
      AND dq_fare_out_of_range = TRUE
) AS source
ON target.ride_id = source.ride_id
   AND target.event_type = source.event_type
WHEN MATCHED THEN
    UPDATE SET
        target.fare_amount          = source.corrected_fare,
        target.net_fare             = ROUND(source.corrected_fare * 0.82, 2),
        target.dq_fare_out_of_range = FALSE,
        target.has_data_quality_issue = FALSE,
        target.silver_processed_ts  = source.silver_processed_ts;


-- ===========================================================================
-- 11. DELTA TIME TRAVEL: Audit Who Changed Records
-- ===========================================================================

DESCRIBE HISTORY rideflow_silver.ride_events_clean;

-- Read data as it was at a specific version
SELECT COUNT(*) AS row_count_at_v10
FROM rideflow_silver.ride_events_clean VERSION AS OF 10;

-- Read data as it was 48 hours ago
SELECT city, COUNT(*) AS rides
FROM rideflow_silver.ride_events_clean TIMESTAMP AS OF (CURRENT_TIMESTAMP() - INTERVAL 48 HOURS)
GROUP BY city;


-- ===========================================================================
-- 12. PERFORMANCE: Z-ORDER and Partition Pruning Validation
-- ===========================================================================

-- This query should hit only 1 partition (event_date filter → partition prune)
-- and benefit from Z-ORDER on ride_id
EXPLAIN FORMATTED
SELECT *
FROM rideflow_silver.ride_events_clean
WHERE event_date = '2024-01-15'
  AND city = 'New York'
  AND ride_id = 'RIDE-ABC123XY';
