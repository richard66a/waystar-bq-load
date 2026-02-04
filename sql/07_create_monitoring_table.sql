-- =============================================================================
-- FTP Log Pipeline - Step 7: Create Monitoring Snapshot Table
-- =============================================================================
-- Stores point-in-time health snapshots for alerting and trend analysis.
--
-- Usage:
--   bq query --use_legacy_sql=false < 07_create_monitoring_table.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS `sbox-ravelar-001-20250926.logviewer.pipeline_monitoring`
(
    check_time TIMESTAMP NOT NULL
        OPTIONS(description = 'Snapshot timestamp'),
    files_processed_last_hour INT64
        OPTIONS(description = 'Files processed in last hour'),
    files_pending INT64
        OPTIONS(description = 'Unprocessed files in GCS'),
    error_rate_pct FLOAT64
        OPTIONS(description = 'Error rate over last 24h'),
    max_latency_minutes INT64
        OPTIONS(description = 'Max processing latency in minutes'),
    stale_file_count INT64
        OPTIONS(description = 'Count of files pending > 30 minutes'),
    status STRING
        OPTIONS(description = 'OK or ALERT'),
    details STRING
        OPTIONS(description = 'Freeform alert details')
)
OPTIONS (
    description = 'Point-in-time pipeline health snapshots for alerting'
);
