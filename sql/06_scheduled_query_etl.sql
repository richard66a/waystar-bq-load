-- =============================================================================
-- FTP Log Pipeline - Step 6: Scheduled Query ETL
-- =============================================================================
-- This is the main ETL query that runs every 5 minutes to process new files.
-- 
-- Pipeline steps:
--   1. Identify new files (not in processed_files table)
--   2. Parse JSON and INSERT structured data into base_ftplog
--   3. Archive raw JSON into archive_ftplog
--   4. Record processed files in processed_files table
--
-- Design decisions:
--   - Uses SAFE_ functions for fault-tolerant parsing
--   - Processes files atomically (all-or-nothing per file)
--   - Empty/whitespace lines are skipped
--   - Failed JSON parsing doesn't crash the pipeline
--
-- To create as scheduled query:
--   bq query --use_legacy_sql=false \
--     --display_name="process_ftplog_files" \
--     --schedule="every 5 minutes" \
--     --destination_table="" \
--     < 06_scheduled_query_etl.sql
--
-- Or run manually for testing:
--   bq query --use_legacy_sql=false < 06_scheduled_query_etl.sql
-- =============================================================================

-- =============================================================================
-- STEP 1: Identify new files not yet processed
-- =============================================================================
-- Create a temp table of files we need to process
-- This avoids processing the same file multiple times
CREATE TEMP TABLE _new_files AS
SELECT DISTINCT
    _FILE_NAME AS file_path,
    REGEXP_EXTRACT(_FILE_NAME, r'/([^/]+)\.json$') AS originating_filename
FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`
WHERE _FILE_NAME IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 
      FROM `sbox-ravelar-001-20250926.logviewer.processed_files` pf
      WHERE pf.gcs_uri = _FILE_NAME
  );

-- Log how many new files found (visible in job details)
SELECT 
    COUNT(*) AS new_files_to_process,
    CURRENT_TIMESTAMP() AS run_timestamp
FROM _new_files;

-- =============================================================================
-- STEP 2: Parse and load structured data into base table
-- =============================================================================
-- Insert parsed JSON data into the base table
-- Uses SAFE_ functions to handle malformed data gracefully
INSERT INTO `sbox-ravelar-001-20250926.logviewer.base_ftplog`
(
    load_time_dt,
    source_file_dt,
    originating_filename,
    gcs_uri,
    action,
    bytes,
    cust_id,
    event_dt,
    filename,
    hash_code,
    ip_address,
    partner_name,
    session_id,
    source,
    user_name,
    server_response,
    raw_data
)
SELECT
    -- Metadata: when we loaded this record
    CURRENT_TIMESTAMP() AS load_time_dt,
    
    -- Metadata: derive from event timestamp or use current time
    COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_VALUE(ext.data, '$.EventDt')),
        CURRENT_TIMESTAMP()
    ) AS source_file_dt,
    
    -- Metadata: filename without extension
    nf.originating_filename,
    
    -- Metadata: full GCS path for traceability
    ext._FILE_NAME AS gcs_uri,
    
    -- Event data: human-readable action
    JSON_VALUE(ext.data, '$.Action') AS action,
    
    -- Event data: bytes transferred (SAFE_CAST handles non-numeric)
    SAFE_CAST(JSON_VALUE(ext.data, '$.Bytes') AS INT64) AS bytes,
    
    -- Event data: customer ID (0 for partners)
    SAFE_CAST(JSON_VALUE(ext.data, '$.CustId') AS INT64) AS cust_id,
    
    -- Event data: when the FTP event occurred
    -- Handles ISO 8601 format: 2026-01-26T10:30:00 or with microseconds
    SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_VALUE(ext.data, '$.EventDt')) AS event_dt,
    
    -- Event data: file path or "-"
    JSON_VALUE(ext.data, '$.Filename') AS filename,
    
    -- Event data: hash for deduplication
    SAFE_CAST(JSON_VALUE(ext.data, '$.HashCode') AS INT64) AS hash_code,
    
    -- Event data: client IP
    JSON_VALUE(ext.data, '$.IpAddress') AS ip_address,
    
    -- Event data: partner name (null for customers)
    JSON_VALUE(ext.data, '$.PartnerName') AS partner_name,
    
    -- Event data: session ID for grouping
    JSON_VALUE(ext.data, '$.SessionId') AS session_id,
    
    -- Event data: FTP server identifier
    JSON_VALUE(ext.data, '$.Source') AS source,
    
    -- Event data: username
    JSON_VALUE(ext.data, '$.UserName') AS user_name,
    
    -- Event data: human-readable status (replaces StatusCode)
    JSON_VALUE(ext.data, '$.ServerResponse') AS server_response,
    
    -- Event data: original log line
    JSON_VALUE(ext.data, '$.RawData') AS raw_data
    
    -- NOTE: StatusCode intentionally NOT included per requirements
    
FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files` ext
INNER JOIN _new_files nf 
    ON ext._FILE_NAME = nf.file_path
WHERE 
    -- Skip empty or whitespace-only lines
    ext.data IS NOT NULL 
    AND TRIM(ext.data) != ''
    -- Basic JSON validation: should start with {
    AND STARTS_WITH(TRIM(ext.data), '{');

-- =============================================================================
-- STEP 3: Archive raw JSON for compliance and recovery
-- =============================================================================
-- Store the raw JSON lines without any transformation
INSERT INTO `sbox-ravelar-001-20250926.logviewer.archive_ftplog`
(
    raw_json,
    archived_timestamp,
    process_dt,
    originating_filename,
    gcs_uri
)
SELECT
    ext.data AS raw_json,
    CURRENT_TIMESTAMP() AS archived_timestamp,
    CURRENT_TIMESTAMP() AS process_dt,
    nf.originating_filename,
    ext._FILE_NAME AS gcs_uri
FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files` ext
INNER JOIN _new_files nf 
    ON ext._FILE_NAME = nf.file_path
WHERE 
    ext.data IS NOT NULL 
    AND TRIM(ext.data) != '';

-- =============================================================================
-- STEP 4: Mark files as processed
-- =============================================================================
-- Record each processed file with row counts and status
INSERT INTO `sbox-ravelar-001-20250926.logviewer.processed_files`
(
    gcs_uri,
    originating_filename,
    processed_timestamp,
    rows_loaded,
    status
)
SELECT
    nf.file_path AS gcs_uri,
    nf.originating_filename,
    CURRENT_TIMESTAMP() AS processed_timestamp,
    -- Count rows loaded for this specific file
    (
        SELECT COUNT(*) 
        FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog` b
        WHERE b.gcs_uri = nf.file_path
          AND b.load_time_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    ) AS rows_loaded,
    'SUCCESS' AS status
FROM _new_files nf;

-- =============================================================================
-- STEP 5: Summary output (visible in job details)
-- =============================================================================
SELECT
    'ETL Complete' AS status,
    CURRENT_TIMESTAMP() AS completed_at,
    (SELECT COUNT(*) FROM _new_files) AS files_processed,
    (
        SELECT COUNT(*) 
        FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
        WHERE load_time_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    ) AS rows_loaded_last_hour;
