
/* ----------------------------------------------------------------------
   1. Top 10 assets with the highest energy consumption
      (all-time total; swap the WHERE clause to bound a period, e.g. MTD)
   ---------------------------------------------------------------------- */
SELECT
    t.asset_id,
    a.asset_name,
    a.asset_type,
    a.site_id,
    ROUND(SUM(t.power_consumption), 2) AS total_energy_consumption
FROM iot_telemetry t
JOIN asset_metadata a
    ON t.asset_id = a.asset_id
GROUP BY t.asset_id, a.asset_name, a.asset_type, a.site_id
ORDER BY total_energy_consumption DESC
LIMIT 10;


/* ----------------------------------------------------------------------
   2. Average daily energy consumption for each site
      Step 1: roll telemetry up to asset-day totals (sensors report at
              sub-daily granularity, so sum first, then average across days)
   ---------------------------------------------------------------------- */
WITH daily_site_consumption AS (
    SELECT
        site_id,
        DATE(timestamp)              AS reading_date,
        SUM(power_consumption)       AS daily_total
    FROM iot_telemetry
    GROUP BY site_id, DATE(timestamp)
)
SELECT
    site_id,
    ROUND(AVG(daily_total), 2) AS avg_daily_energy_consumption,
    COUNT(*)                   AS days_observed
FROM daily_site_consumption
GROUP BY site_id
ORDER BY avg_daily_energy_consumption DESC;


/* ----------------------------------------------------------------------
   3. Assets that generated more than 10 faults in the last 30 days
   ---------------------------------------------------------------------- */
SELECT
    e.asset_id,
    a.asset_name,
    a.site_id,
    COUNT(*) AS fault_count
FROM event e
JOIN asset_metadata a
    ON e.asset_id = a.asset_id
WHERE e.event_type = 'Fault'
  AND e.timestamp >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
GROUP BY e.asset_id, a.asset_name, a.site_id
HAVING COUNT(*) > 10
ORDER BY fault_count DESC;


/* ----------------------------------------------------------------------
   4. Assets that have NOT reported telemetry in the last 24 hours
      Anti-join every known asset against telemetry received in the window,
      so an asset that has *never* reported also shows up (not just ones
      that went silent).
   ---------------------------------------------------------------------- */
WITH recent_reporters AS (
    SELECT DISTINCT asset_id
    FROM iot_telemetry
    WHERE timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
),
last_seen AS (
    SELECT asset_id, MAX(timestamp) AS last_reading_ts
    FROM iot_telemetry
    GROUP BY asset_id
)
SELECT
    a.asset_id,
    a.asset_name,
    a.site_id,
    ls.last_reading_ts
FROM asset_metadata a
LEFT JOIN recent_reporters r
    ON a.asset_id = r.asset_id
LEFT JOIN last_seen ls
    ON a.asset_id = ls.asset_id
WHERE r.asset_id IS NULL
ORDER BY ls.last_reading_ts ASC NULLS FIRST;


/* ----------------------------------------------------------------------
   5. Hourly utilization for each building
      Utilization = % of telemetry readings where the asset was actively
      operating (operating_mode = 'Running') out of all readings received
      in that hour. Swap the CASE condition if "Running" should include
      other active states (e.g. 'Running','Ramping').
   ---------------------------------------------------------------------- */
SELECT
    building_id,
    DATE_TRUNC('HOUR', timestamp)                          AS hour_bucket,
    COUNT(*)                                                AS total_readings,
    SUM(CASE WHEN operating_mode = 'Running' THEN 1 ELSE 0 END) AS active_readings,
    ROUND(
        100.0 * SUM(CASE WHEN operating_mode = 'Running' THEN 1 ELSE 0 END)
        / COUNT(*), 2
    ) AS utilization_pct
FROM iot_telemetry
GROUP BY building_id, DATE_TRUNC('HOUR', timestamp)
ORDER BY building_id, hour_bucket;


/* ----------------------------------------------------------------------
   6. Sites with abnormal increases in power consumption
      Method: compare each site-day's total consumption against that
      site's trailing 7-day rolling average + 2 standard deviations
      (a simple, explainable anomaly-detection baseline; could be swapped
      for an ML-based approach for production).
   ---------------------------------------------------------------------- */
WITH daily_site_consumption AS (
    SELECT
        site_id,
        DATE(timestamp)        AS reading_date,
        SUM(power_consumption) AS daily_total
    FROM iot_telemetry
    GROUP BY site_id, DATE(timestamp)
),
rolling_stats AS (
    SELECT
        site_id,
        reading_date,
        daily_total,
        AVG(daily_total) OVER (
            PARTITION BY site_id
            ORDER BY reading_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg_7d,
        STDDEV(daily_total) OVER (
            PARTITION BY site_id
            ORDER BY reading_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS rolling_stddev_7d
    FROM daily_site_consumption
)
SELECT
    site_id,
    reading_date,
    daily_total,
    ROUND(rolling_avg_7d, 2)                          AS baseline_avg_7d,
    ROUND(rolling_avg_7d + 2 * rolling_stddev_7d, 2)   AS anomaly_threshold,
    ROUND(daily_total - rolling_avg_7d, 2)             AS delta_from_baseline
FROM rolling_stats
WHERE rolling_avg_7d IS NOT NULL
  AND daily_total > rolling_avg_7d + 2 * rolling_stddev_7d
ORDER BY site_id, reading_date;
