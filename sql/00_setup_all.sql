-- =============================================================================
-- FTP Log Pipeline - Combined Setup Script
-- =============================================================================
-- Run all table creation scripts in one execution.
-- This is the preferred method for initial setup.
--
-- Usage:
--   bq query --use_legacy_sql=false < 00_setup_all.sql
--
-- Prerequisites:
--   - GCP project exists and you have access
--   - BigQuery API is enabled
--   - GCS bucket exists (for external table)
-- =============================================================================

-- =====================================================================
-- STEP 1: Create Dataset
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS `__PROJECT_ID__.__DATASET_ID__`
OPTIONS (
    description = 'FTP log data - GCP-native pipeline migrated from Snowflake',
    location = 'US',
    labels = [
        ('environment', 'sandbox'),
        ('team', 'platform-arch'),
        ('pipeline', 'ftplog')
    ]
);

-- =====================================================================
-- STEP 2: Create Base Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET_ID__.base_ftplog`
(
    load_time_dt TIMESTAMP NOT NULL 
        OPTIONS(description = 'Timestamp when this record was loaded to BigQuery'),
    source_file_dt TIMESTAMP 
        OPTIONS(description = 'Timestamp derived from source file events'),
    originating_filename STRING NOT NULL 
        OPTIONS(description = 'Source file loadId (filename without extension)'),
    gcs_uri STRING 
        OPTIONS(description = 'Full GCS path: gs://bucket/logs/filename.json'),
    action STRING 
        OPTIONS(description = 'Human-readable FTP action'),
    bytes INT64 
        OPTIONS(description = 'Bytes transferred'),
    cust_id INT64 
        OPTIONS(description = 'Customer ID (0 if partner login)'),
    event_dt TIMESTAMP 
        OPTIONS(description = 'Event timestamp from FTP server log'),
    filename STRING 
        OPTIONS(description = 'File path or "-" for non-file operations'),
    hash_code INT64 
        OPTIONS(description = 'Computed hash for row-level deduplication'),
    hash_fingerprint STRING 
        OPTIONS(description = 'Deterministic SHA256 hex fingerprint for row deduplication'),
    ip_address STRING 
        OPTIONS(description = 'Client IP address'),
    partner_name STRING 
        OPTIONS(description = 'Partner name (null if customer)'),
    session_id STRING 
        OPTIONS(description = 'FTP session identifier'),
    source STRING 
        OPTIONS(description = 'FTP server identifier'),
    user_name STRING 
        OPTIONS(description = 'FTP username'),
    server_response STRING 
        OPTIONS(description = 'Human-readable FTP status message'),
    raw_data STRING 
        OPTIONS(description = 'Original FTP log line')
)
PARTITION BY DATE(event_dt)
CLUSTER BY source, cust_id
OPTIONS (
    description = 'FTP log events - partitioned by event date, clustered by source and customer',
    labels = [('table_type', 'base'), ('retention', 'standard')]
);

-- =====================================================================
-- STEP 3: Create Archive Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET_ID__.archive_ftplog`
(
    raw_json STRING NOT NULL 
        OPTIONS(description = 'Complete raw JSON line from source file'),
    archived_timestamp TIMESTAMP NOT NULL 
        OPTIONS(description = 'When this record was archived'),
    process_dt TIMESTAMP NOT NULL 
        OPTIONS(description = 'ETL processing timestamp'),
    originating_filename STRING NOT NULL 
        OPTIONS(description = 'Source file loadId'),
    gcs_uri STRING 
        OPTIONS(description = 'Full GCS path for recovery')
)
PARTITION BY DATE(archived_timestamp)
OPTIONS (
    description = 'Permanent archive of raw JSON FTP log events - NEVER DELETE',
    labels = [('table_type', 'archive'), ('retention', 'permanent')]
);

-- =====================================================================
-- STEP 4: Create Processed Files Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET_ID__.processed_files`
(
    gcs_uri STRING NOT NULL 
        OPTIONS(description = 'Full GCS URI of processed file'),
    originating_filename STRING NOT NULL 
        OPTIONS(description = 'LoadId extracted from filename'),
    processed_timestamp TIMESTAMP NOT NULL 
        OPTIONS(description = 'When file was processed'),
    rows_loaded INT64 
        OPTIONS(description = 'Number of rows loaded from this file'),
    rows_expected INT64 
        OPTIONS(description = 'Number of non-empty rows discovered for this file'),
    parse_errors INT64 
        OPTIONS(description = 'Number of rows not loaded (rows_expected - rows_loaded)'),
    status STRING 
        OPTIONS(description = 'Processing status: SUCCESS, FAILED, PARTIAL'),
    error_message STRING 
        OPTIONS(description = 'Error details if processing failed'),
    processing_duration_seconds FLOAT64 
        OPTIONS(description = 'Time taken to process this file')
)
OPTIONS (
    description = 'Tracks processed GCS files to prevent duplicate loading',
    labels = [('table_type', 'tracking'), ('purpose', 'idempotency')]
);

-- Ensure schema is updated if the table already exists
ALTER TABLE `__PROJECT_ID__.__DATASET_ID__.processed_files`
ADD COLUMN IF NOT EXISTS rows_expected INT64 OPTIONS(description = 'Number of non-empty rows discovered for this file');

ALTER TABLE `__PROJECT_ID__.__DATASET_ID__.processed_files`
ADD COLUMN IF NOT EXISTS parse_errors INT64 OPTIONS(description = 'Number of rows not loaded (rows_expected - rows_loaded)');

ALTER TABLE `__PROJECT_ID__.__DATASET_ID__.base_ftplog`
ADD COLUMN IF NOT EXISTS hash_fingerprint STRING OPTIONS(description = 'Deterministic SHA256 hex fingerprint for row deduplication');

-- =====================================================================
-- STEP 5: Create External Table
-- =====================================================================
-- =====================================================================
-- STEP 5: Create Monitoring Snapshot Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET_ID__.pipeline_monitoring`
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

-- =====================================================================
-- STEP 6: Create External Table
-- =====================================================================
-- Drop if exists (external tables can't use IF NOT EXISTS with different URIs)
DROP EXTERNAL TABLE IF EXISTS `__PROJECT_ID__.__DATASET_ID__.external_ftplog_files`;

CREATE EXTERNAL TABLE `__PROJECT_ID__.__DATASET_ID__.external_ftplog_files`
(
    data STRING OPTIONS(description = 'Raw JSON line from NDJSON file')
)
OPTIONS (
    format = 'CSV',
    field_delimiter = '\u001F',
    skip_leading_rows = 0,
    allow_quoted_newlines = true,
    -- Placeholder token: __GCS_URI__  (will be replaced at deploy time by scripts/deploy.sh)
    uris = ['__GCS_URI__'],
    description = 'External table reading NDJSON files from GCS as raw lines'
);

SELECT
    table_name,
    table_type,
    creation_time AS created_at
FROM `__PROJECT_ID__.__DATASET_ID__.INFORMATION_SCHEMA.TABLES`
ORDER BY creation_time DESC;
