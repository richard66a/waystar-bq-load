-- =============================================================================
-- FTP Log Pipeline - Operational Runbook: Monitoring & Alerting
-- =============================================================================
-- Queries for monitoring pipeline health and creating alerting conditions.
-- These can be used with BigQuery scheduled queries + Cloud Monitoring.
-- =============================================================================


-- =============================================================================
-- MONITOR 1: Pipeline Heartbeat
-- =============================================================================
-- Check if pipeline has processed files in the last hour
-- Alert if: no files processed when files are pending

SELECT
    CURRENT_TIMESTAMP() AS check_time,
    (SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
     WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) AS files_processed_last_hour,
    (SELECT COUNT(DISTINCT _FILE_NAME) FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`
     WHERE NOT EXISTS (
         SELECT 1 FROM `sbox-ravelar-001-20250926.logviewer.processed_files` pf
         WHERE pf.gcs_uri = _FILE_NAME
     )) AS files_pending,
    CASE
        WHEN (SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
              WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) = 0
             AND (SELECT COUNT(DISTINCT _FILE_NAME) FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`
                  WHERE NOT EXISTS (
                      SELECT 1 FROM `sbox-ravelar-001-20250926.logviewer.processed_files` pf
                      WHERE pf.gcs_uri = _FILE_NAME
                  )) > 0
        THEN 'ALERT: Pipeline may be stuck!'
        ELSE 'OK'
    END AS status;


-- =============================================================================
-- MONITOR 2: Processing Latency
-- =============================================================================
-- Calculate time between file arrival and processing
-- Alert if: latency exceeds 15 minutes

WITH file_ages AS (
    SELECT 
        ext._FILE_NAME AS gcs_uri,
        pf.processed_timestamp,
        -- Estimate file creation time from filename pattern
        SAFE.PARSE_TIMESTAMP(
            '%Y%m%d-%H%M%S',
            REGEXP_EXTRACT(ext._FILE_NAME, r'-(\d{8}-\d{6})')
        ) AS estimated_file_time
    FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files` ext
    LEFT JOIN `sbox-ravelar-001-20250926.logviewer.processed_files` pf
        ON pf.gcs_uri = ext._FILE_NAME
)
SELECT
    CURRENT_TIMESTAMP() AS check_time,
    AVG(TIMESTAMP_DIFF(processed_timestamp, estimated_file_time, MINUTE)) AS avg_latency_minutes,
    MAX(TIMESTAMP_DIFF(processed_timestamp, estimated_file_time, MINUTE)) AS max_latency_minutes,
    COUNTIF(TIMESTAMP_DIFF(processed_timestamp, estimated_file_time, MINUTE) > 15) AS files_over_15min,
    CASE
        WHEN MAX(TIMESTAMP_DIFF(processed_timestamp, estimated_file_time, MINUTE)) > 15
        THEN 'WARNING: High latency detected'
        ELSE 'OK'
    END AS status
FROM file_ages
WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND estimated_file_time IS NOT NULL;


-- =============================================================================
-- MONITOR 3: Error Rate
-- =============================================================================
-- Track processing failures
-- Alert if: error rate exceeds 5%

SELECT
    CURRENT_TIMESTAMP() AS check_time,
    COUNT(*) AS total_processed_24h,
    COUNTIF(status = 'SUCCESS') AS successful,
    COUNTIF(status = 'FAILED') AS failed,
    COUNTIF(status = 'PARTIAL') AS partial,
    ROUND(SAFE_DIVIDE(COUNTIF(status != 'SUCCESS'), COUNT(*)) * 100, 2) AS error_rate_pct,
    CASE
        WHEN SAFE_DIVIDE(COUNTIF(status != 'SUCCESS'), COUNT(*)) > 0.05
        THEN 'ALERT: Error rate exceeds 5%!'
        ELSE 'OK'
    END AS status
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);


-- =============================================================================
-- MONITOR 4: Data Volume Anomalies
-- =============================================================================
-- Detect unusual changes in data volume
-- Alert if: volume differs by >50% from 7-day average

WITH daily_volumes AS (
    SELECT
        DATE(processed_timestamp) AS processing_date,
        COUNT(*) AS files,
        SUM(rows_loaded) AS rows
    FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
    WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 8 DAY)
    GROUP BY DATE(processed_timestamp)
),
stats AS (
    SELECT
        AVG(rows) AS avg_daily_rows,
        STDDEV(rows) AS stddev_rows
    FROM daily_volumes
    WHERE processing_date < CURRENT_DATE()
)
SELECT
    CURRENT_TIMESTAMP() AS check_time,
    dv.processing_date,
    dv.rows AS today_rows,
    s.avg_daily_rows,
    ROUND(ABS(dv.rows - s.avg_daily_rows) / NULLIF(s.avg_daily_rows, 0) * 100, 2) AS pct_deviation,
    CASE
        WHEN ABS(dv.rows - s.avg_daily_rows) / NULLIF(s.avg_daily_rows, 0) > 0.5
        THEN 'WARNING: Unusual volume'
        ELSE 'OK'
    END AS status
FROM daily_volumes dv
CROSS JOIN stats s
WHERE dv.processing_date = CURRENT_DATE();


-- =============================================================================
-- MONITOR 5: Stale Files Detection
-- =============================================================================
-- Find files that have been pending for too long
-- Alert if: any file pending > 30 minutes

WITH pending_files AS (
    SELECT 
        ext._FILE_NAME AS gcs_uri,
        SAFE.PARSE_TIMESTAMP(
            '%Y%m%d-%H%M%S',
            REGEXP_EXTRACT(ext._FILE_NAME, r'-(\d{8}-\d{6})')
        ) AS estimated_file_time
    FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files` ext
    WHERE NOT EXISTS (
        SELECT 1 FROM `sbox-ravelar-001-20250926.logviewer.processed_files` pf
        WHERE pf.gcs_uri = ext._FILE_NAME
    )
)
SELECT
    CURRENT_TIMESTAMP() AS check_time,
    COUNT(*) AS stale_file_count,
    MAX(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), estimated_file_time, MINUTE)) AS oldest_pending_minutes,
    ARRAY_AGG(gcs_uri ORDER BY estimated_file_time LIMIT 5) AS sample_stale_files,
    CASE
        WHEN MAX(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), estimated_file_time, MINUTE)) > 30
        THEN 'ALERT: Stale files detected!'
        ELSE 'OK'
    END AS status
FROM pending_files
WHERE estimated_file_time IS NOT NULL;


-- =============================================================================
-- MONITOR 6: Storage Growth
-- =============================================================================
-- Track table sizes over time
-- Alert if: growth rate exceeds expected

SELECT
    table_name,
    ROUND(size_bytes / 1024 / 1024 / 1024, 2) AS size_gb,
    row_count,
    TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `sbox-ravelar-001-20250926.logviewer.__TABLES__`
ORDER BY size_bytes DESC;


-- =============================================================================
-- DASHBOARD QUERY: Overall Health Summary
-- =============================================================================
-- Single query that provides complete pipeline status

SELECT
    'Pipeline Health Dashboard' AS report,
    CURRENT_TIMESTAMP() AS generated_at,
    
    -- Processing stats (last 24h)
    (SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
     WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) AS files_24h,
    (SELECT SUM(rows_loaded) FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
     WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) AS rows_24h,
    
    -- Current backlog
    (SELECT COUNT(DISTINCT _FILE_NAME) FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`
     WHERE NOT EXISTS (
         SELECT 1 FROM `sbox-ravelar-001-20250926.logviewer.processed_files` pf
         WHERE pf.gcs_uri = _FILE_NAME
     )) AS pending_files,
    
    -- Error count
    (SELECT COUNTIF(status != 'SUCCESS') FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
     WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) AS errors_24h,
    
    -- Last processing time
    (SELECT MAX(processed_timestamp) FROM `sbox-ravelar-001-20250926.logviewer.processed_files`) AS last_processed,
    
    -- Overall status
    CASE
        WHEN (SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
              WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) = 0
             AND (SELECT COUNT(DISTINCT _FILE_NAME) FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`
                  WHERE NOT EXISTS (
                      SELECT 1 FROM `sbox-ravelar-001-20250926.logviewer.processed_files` pf
                      WHERE pf.gcs_uri = _FILE_NAME
                  )) > 0
        THEN '🔴 CRITICAL: Pipeline appears stuck'
        WHEN (SELECT COUNTIF(status != 'SUCCESS') FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
              WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) > 
             (SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
              WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) * 0.05
        THEN '🟡 WARNING: High error rate'
        ELSE '🟢 HEALTHY'
    END AS overall_status;
