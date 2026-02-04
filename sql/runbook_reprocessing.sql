-- =============================================================================
-- FTP Log Pipeline - Operational Runbook: Reprocessing Files
-- =============================================================================
-- Use these queries when you need to reprocess files that failed or 
-- need to be re-run for any reason.
--
-- CAUTION: These queries modify data. Always backup or verify first.
-- =============================================================================


-- =============================================================================
-- SCENARIO 1: Reprocess a Single File
-- =============================================================================
-- Use this when a specific file needs to be reprocessed (e.g., ETL bug fixed)

-- Step 1: Identify the file to reprocess
-- Replace with your actual file path
DECLARE file_to_reprocess STRING DEFAULT 'gs://sbox-ravelar-001-20250926-ftplog/logs/YOUR-FILE-NAME.json';

-- Step 2: Check current state
SELECT 
    gcs_uri,
    processed_timestamp,
    rows_loaded,
    status
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
WHERE gcs_uri = file_to_reprocess;

-- Step 3: Remove from processed_files (allows reprocessing)
DELETE FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
WHERE gcs_uri = file_to_reprocess;

-- Step 4: Remove existing data for this file from base table
DELETE FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
WHERE gcs_uri = file_to_reprocess;

-- Step 5: Remove from archive (optional - usually keep for compliance)
-- DELETE FROM `sbox-ravelar-001-20250926.logviewer.archive_ftplog`
-- WHERE gcs_uri = file_to_reprocess;

-- Step 6: File will be reprocessed on next ETL run
-- Or run ETL manually: bq query --use_legacy_sql=false < sql/06_scheduled_query_etl.sql


-- =============================================================================
-- SCENARIO 2: Reprocess All Failed Files
-- =============================================================================
-- Use this to retry all files that previously failed

-- Step 1: Identify failed files
SELECT 
    gcs_uri,
    processed_timestamp,
    status,
    error_message
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
WHERE status IN ('FAILED', 'PARTIAL')
ORDER BY processed_timestamp DESC;

-- Step 2: Remove failed files from tracking (for reprocessing)
DELETE FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
WHERE status IN ('FAILED', 'PARTIAL');

-- Step 3: Clean up any partial data
-- This removes rows from files that partially failed
DELETE FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
WHERE gcs_uri IN (
    SELECT DISTINCT gcs_uri 
    FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
    WHERE status = 'PARTIAL'
);


-- =============================================================================
-- SCENARIO 3: Reprocess Files from a Date Range
-- =============================================================================
-- Use this to reprocess all files from a specific time period

DECLARE start_date TIMESTAMP DEFAULT TIMESTAMP('2026-01-28 00:00:00');
DECLARE end_date TIMESTAMP DEFAULT TIMESTAMP('2026-01-28 23:59:59');

-- Step 1: Identify files in date range
SELECT 
    gcs_uri,
    processed_timestamp,
    rows_loaded,
    status
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
WHERE processed_timestamp BETWEEN start_date AND end_date
ORDER BY processed_timestamp;

-- Step 2: Remove from processed_files
DELETE FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
WHERE processed_timestamp BETWEEN start_date AND end_date;

-- Step 3: Remove corresponding base data
DELETE FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
WHERE load_time_dt BETWEEN start_date AND end_date;

-- Step 4: Optionally remove archive data (usually don't)
-- DELETE FROM `sbox-ravelar-001-20250926.logviewer.archive_ftplog`
-- WHERE process_dt BETWEEN start_date AND end_date;


-- =============================================================================
-- SCENARIO 4: Force Reprocess ALL Files (Full Reload)
-- =============================================================================
-- DANGER: This will reprocess everything! Use only for major schema changes.

-- Step 1: Verify you really want to do this
SELECT 
    'FULL RELOAD WARNING' AS warning,
    COUNT(*) AS files_to_reprocess,
    SUM(rows_loaded) AS rows_to_delete
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`;

-- Step 2: Clear all tracking
TRUNCATE TABLE `sbox-ravelar-001-20250926.logviewer.processed_files`;

-- Step 3: Clear base table
TRUNCATE TABLE `sbox-ravelar-001-20250926.logviewer.base_ftplog`;

-- Step 4: Clear archive (optional - usually keep)
-- TRUNCATE TABLE `sbox-ravelar-001-20250926.logviewer.archive_ftplog`;

-- Step 5: Run ETL - will process all files from scratch


-- =============================================================================
-- SCENARIO 5: Handle Duplicate Processing
-- =============================================================================
-- Use when files were accidentally processed multiple times

-- Step 1: Find duplicates in processed_files
SELECT 
    gcs_uri,
    COUNT(*) AS times_processed,
    ARRAY_AGG(processed_timestamp ORDER BY processed_timestamp) AS processing_times
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
GROUP BY gcs_uri
HAVING COUNT(*) > 1;

-- Step 2: Keep only the latest processing record
DELETE FROM `sbox-ravelar-001-20250926.logviewer.processed_files` pf1
WHERE EXISTS (
    SELECT 1 
    FROM `sbox-ravelar-001-20250926.logviewer.processed_files` pf2
    WHERE pf2.gcs_uri = pf1.gcs_uri
      AND pf2.processed_timestamp > pf1.processed_timestamp
);

-- Step 3: Deduplicate base table using hash_code and gcs_uri
-- Keep earliest load_time_dt for each unique combination
CREATE OR REPLACE TABLE `sbox-ravelar-001-20250926.logviewer.base_ftplog` AS
SELECT * EXCEPT(row_num)
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY gcs_uri, hash_code 
            ORDER BY load_time_dt
        ) AS row_num
    FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
)
WHERE row_num = 1;
