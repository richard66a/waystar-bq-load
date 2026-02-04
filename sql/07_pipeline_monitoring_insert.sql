-- =============================================================================
-- FTP Log Pipeline - Monitoring Snapshot Insert (DML-only)
-- =============================================================================
-- Intended for use in a scheduled query to populate pipeline_monitoring.
-- =============================================================================

INSERT INTO `sbox-ravelar-001-20250926.logviewer.pipeline_monitoring`
(
    check_time,
    files_processed_last_hour,
    files_pending,
    error_rate_pct,
    max_latency_minutes,
    stale_file_count,
    status,
    details
)
WITH
pending_files AS (
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
),
stats AS (
    SELECT
        (SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
         WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)) AS files_processed_last_hour,
        (SELECT COUNT(*) FROM pending_files) AS files_pending,
        (SELECT ROUND(SAFE_DIVIDE(COUNTIF(status != 'SUCCESS'), COUNT(*)) * 100, 2)
         FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
         WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)) AS error_rate_pct,
        (SELECT MAX(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), estimated_file_time, MINUTE))
         FROM pending_files
         WHERE estimated_file_time IS NOT NULL) AS max_latency_minutes,
        (SELECT COUNT(*)
         FROM pending_files
         WHERE estimated_file_time IS NOT NULL
           AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), estimated_file_time, MINUTE) > 30) AS stale_file_count
)
SELECT
    CURRENT_TIMESTAMP() AS check_time,
    files_processed_last_hour,
    files_pending,
    error_rate_pct,
    max_latency_minutes,
    stale_file_count,
    CASE
        WHEN (files_processed_last_hour = 0 AND files_pending > 0)
          OR (error_rate_pct > 5)
          OR (COALESCE(max_latency_minutes, 0) > 15)
          OR (stale_file_count > 0)
        THEN 'ALERT'
        ELSE 'OK'
    END AS status,
    CONCAT(
        'processed_last_hour=', COALESCE(CAST(files_processed_last_hour AS STRING), '0'),
        ', pending=', COALESCE(CAST(files_pending AS STRING), '0'),
        ', error_rate_pct=', COALESCE(CAST(error_rate_pct AS STRING), '0'),
        ', max_latency_min=', COALESCE(CAST(max_latency_minutes AS STRING), '0'),
        ', stale_files=', COALESCE(CAST(stale_file_count AS STRING), '0')
    ) AS details
FROM stats;
