-- =============================================================================
-- FTP Log Pipeline - Validation Queries
-- =============================================================================
-- These queries are used to validate the pipeline is working correctly.
-- Run these after processing test files to verify data integrity.
--
-- Usage:
--   bq query --use_legacy_sql=false < validation_queries.sql
-- =============================================================================


-- =============================================================================
-- SECTION 1: Pipeline Health Checks
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1.1 Overall Pipeline Status
-- Shows files processed in last 24 hours with success/failure counts
-- -----------------------------------------------------------------------------
SELECT 
    'Pipeline Status - Last 24 Hours' AS report,
    COUNT(*) AS total_files,
    COUNTIF(status = 'SUCCESS') AS successful,
    COUNTIF(status = 'FAILED') AS failed,
    COUNTIF(status = 'PARTIAL') AS partial,
    SUM(rows_loaded) AS total_rows_loaded,
    AVG(rows_loaded) AS avg_rows_per_file
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);


-- -----------------------------------------------------------------------------
-- 1.2 Files Pending Processing
-- Shows files in GCS that haven't been processed yet
-- -----------------------------------------------------------------------------
SELECT 
    'Unprocessed Files' AS report,
    COUNT(DISTINCT _FILE_NAME) AS pending_files
FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`
WHERE NOT EXISTS (
    SELECT 1 
    FROM `sbox-ravelar-001-20250926.logviewer.processed_files` pf
    WHERE pf.gcs_uri = _FILE_NAME
);


-- -----------------------------------------------------------------------------
-- 1.3 Recent Processing Activity
-- Shows the last 10 files processed
-- -----------------------------------------------------------------------------
SELECT 
    originating_filename,
    processed_timestamp,
    rows_loaded,
    status,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), processed_timestamp, MINUTE) AS minutes_ago
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
ORDER BY processed_timestamp DESC
LIMIT 10;


-- =============================================================================
-- SECTION 2: Data Quality Checks
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2.1 Row Count Validation
-- Compares counts across tables for a specific file
-- Replace the gcs_uri with your test file
-- -----------------------------------------------------------------------------
DECLARE test_file STRING DEFAULT 'gs://sbox-ravelar-001-20250926-ftplog/logs/FTP-SERVER-01-20260128-103000001-test-sample-001.json';

SELECT 
    'Row Count Validation' AS check_name,
    (SELECT rows_loaded FROM `sbox-ravelar-001-20250926.logviewer.processed_files` WHERE gcs_uri = test_file) AS tracked_rows,
    (SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog` WHERE gcs_uri = test_file) AS base_table_rows,
    (SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.archive_ftplog` WHERE gcs_uri = test_file) AS archive_rows;


-- -----------------------------------------------------------------------------
-- 2.2 Null Check for Required Fields
-- Identifies any rows with null values in required fields
-- -----------------------------------------------------------------------------
SELECT 
    'Null Value Check' AS check_name,
    COUNT(*) AS total_rows,
    COUNTIF(load_time_dt IS NULL) AS null_load_time,
    COUNTIF(originating_filename IS NULL) AS null_filename,
    COUNTIF(event_dt IS NULL) AS null_event_dt,
    COUNTIF(source IS NULL) AS null_source,
    COUNTIF(action IS NULL) AS null_action
FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
WHERE load_time_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);


-- -----------------------------------------------------------------------------
-- 2.3 Data Type Validation
-- Checks for parsing issues (failed SAFE_CAST results in NULL)
-- -----------------------------------------------------------------------------
SELECT
    'Parse Error Check' AS check_name,
    COUNT(*) AS total_rows,
    COUNTIF(bytes IS NULL AND raw_data LIKE '%Bytes%') AS possible_bytes_parse_errors,
    COUNTIF(cust_id IS NULL AND raw_data LIKE '%CustId%') AS possible_custid_parse_errors,
    COUNTIF(hash_code IS NULL AND raw_data LIKE '%HashCode%') AS possible_hash_parse_errors
FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
WHERE load_time_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);


-- =============================================================================
-- SECTION 3: Duplicate Detection
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 3.1 Duplicate File Processing Check
-- Identifies if any file was processed more than once
-- -----------------------------------------------------------------------------
SELECT 
    'Duplicate File Check' AS check_name,
    gcs_uri,
    COUNT(*) AS times_processed
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
GROUP BY gcs_uri
HAVING COUNT(*) > 1
ORDER BY times_processed DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 3.2 Duplicate Row Check Using Hash
-- Identifies potential duplicate events using hash_code
-- -----------------------------------------------------------------------------
SELECT 
    'Duplicate Row Check (by hash)' AS check_name,
    hash_code,
    COUNT(*) AS occurrences
FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
WHERE load_time_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND hash_code IS NOT NULL
GROUP BY hash_code
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 10;


-- =============================================================================
-- SECTION 4: Volume and Performance Metrics
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 4.1 Daily Processing Volume
-- Shows files and rows processed per day
-- -----------------------------------------------------------------------------
SELECT
    DATE(processed_timestamp) AS processing_date,
    COUNT(*) AS files_processed,
    SUM(rows_loaded) AS total_rows,
    AVG(rows_loaded) AS avg_rows_per_file,
    MIN(rows_loaded) AS min_rows,
    MAX(rows_loaded) AS max_rows
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
GROUP BY DATE(processed_timestamp)
ORDER BY processing_date DESC
LIMIT 30;


-- -----------------------------------------------------------------------------
-- 4.2 Hourly Processing Distribution
-- Shows processing activity by hour
-- -----------------------------------------------------------------------------
SELECT
    EXTRACT(HOUR FROM processed_timestamp) AS hour_of_day,
    COUNT(*) AS files_processed,
    SUM(rows_loaded) AS total_rows
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
WHERE processed_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY hour_of_day
ORDER BY hour_of_day;


-- -----------------------------------------------------------------------------
-- 4.3 Source Server Distribution
-- Shows volume by FTP server source
-- -----------------------------------------------------------------------------
SELECT
    source,
    COUNT(*) AS event_count,
    COUNT(DISTINCT DATE(event_dt)) AS active_days,
    MIN(event_dt) AS first_event,
    MAX(event_dt) AS last_event
FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
WHERE event_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY source
ORDER BY event_count DESC;


-- =============================================================================
-- SECTION 5: Business Data Validation
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 5.1 Customer vs Partner Distribution
-- Shows breakdown of customer vs partner activity
-- -----------------------------------------------------------------------------
SELECT
    'User Type Distribution' AS check_name,
    COUNTIF(cust_id > 0) AS customer_events,
    COUNTIF(cust_id = 0 AND partner_name IS NOT NULL) AS partner_events,
    COUNTIF(cust_id = 0 AND partner_name IS NULL) AS unknown_events,
    COUNT(DISTINCT CASE WHEN cust_id > 0 THEN cust_id END) AS unique_customers,
    COUNT(DISTINCT partner_name) AS unique_partners
FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
WHERE event_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY);


-- -----------------------------------------------------------------------------
-- 5.2 Action Type Distribution
-- Shows breakdown of FTP actions
-- -----------------------------------------------------------------------------
SELECT
    action,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
WHERE event_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY action
ORDER BY event_count DESC;


-- -----------------------------------------------------------------------------
-- 5.3 Sample Data Inspection
-- Shows sample records for visual verification
-- -----------------------------------------------------------------------------
SELECT
    event_dt,
    source,
    user_name,
    cust_id,
    partner_name,
    action,
    filename,
    bytes,
    server_response
FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
ORDER BY event_dt DESC
LIMIT 20;


-- =============================================================================
-- SECTION 6: Archive Validation
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 6.1 Archive Completeness Check
-- Verifies archive has all the raw data
-- -----------------------------------------------------------------------------
SELECT
    'Archive vs Base Comparison' AS check_name,
    (SELECT COUNT(DISTINCT gcs_uri) FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`) AS base_files,
    (SELECT COUNT(DISTINCT gcs_uri) FROM `sbox-ravelar-001-20250926.logviewer.archive_ftplog`) AS archive_files,
    (SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`) AS base_rows,
    (SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.archive_ftplog`) AS archive_rows;


-- -----------------------------------------------------------------------------
-- 6.2 Archive JSON Validity Check
-- Samples archive records to verify JSON is parseable
-- -----------------------------------------------------------------------------
SELECT
    originating_filename,
    SAFE.PARSE_JSON(raw_json) IS NOT NULL AS is_valid_json,
    LEFT(raw_json, 100) AS json_preview
FROM `sbox-ravelar-001-20250926.logviewer.archive_ftplog`
ORDER BY archived_timestamp DESC
LIMIT 10;
