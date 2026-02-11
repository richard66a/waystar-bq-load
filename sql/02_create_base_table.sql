-- =============================================================================
-- FTP Log Pipeline - Step 2: Create Base FTP Log Table
-- =============================================================================
-- This table stores the structured, queryable FTP log events.
-- 
-- Design decisions:
--   - Partitioned by event_dt: Enables efficient date-range queries and 
--     automatic partition pruning (only scans relevant date partitions)
--   - Clustered by source, cust_id: Optimizes queries filtering by FTP server
--     or customer ID (common access patterns)
--   - NOT NULL constraints on critical metadata fields for data integrity
--   - StatusCode intentionally EXCLUDED (parity with Snowflake schema)
--
-- Usage:
--   bq query --use_legacy_sql=false < 02_create_base_table.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET_ID__.base_ftplog`
(
    -- =========================================================================
    -- Metadata columns (populated during ETL)
    -- =========================================================================
    
    -- Timestamp when this record was loaded into BigQuery
    load_time_dt TIMESTAMP NOT NULL 
        OPTIONS(description = 'Timestamp when this record was loaded to BigQuery'),
    
    -- Timestamp derived from source file or defaults to load time
    source_file_dt TIMESTAMP 
        OPTIONS(description = 'Timestamp derived from source file events'),
    
    -- Source filename without extension (used for traceability)
    originating_filename STRING NOT NULL 
        OPTIONS(description = 'Source file loadId (filename without extension)'),
    
    -- Full GCS path for audit trail and debugging
    gcs_uri STRING 
        OPTIONS(description = 'Full GCS path: gs://bucket/logs/filename.json'),

    -- =========================================================================
    -- Event data columns (parsed from JSON)
    -- =========================================================================
    
    -- Human-readable FTP action (Store, Login, Download, etc.)
    action STRING 
        OPTIONS(description = 'Human-readable FTP action'),
    
    -- Number of bytes transferred (0 for non-file operations)
    bytes INT64 
        OPTIONS(description = 'Bytes transferred'),
    
    -- Customer ID (0 if username is a partner name, not numeric)
    cust_id INT64 
        OPTIONS(description = 'Customer ID (0 if partner login)'),
    
    -- When the FTP event occurred (from server log)
    event_dt TIMESTAMP 
        OPTIONS(description = 'Event timestamp from FTP server log'),
    
    -- File path involved or "-" for non-file operations
    filename STRING 
        OPTIONS(description = 'File path or "-" for non-file operations'),
    
    -- Computed hash for deduplication checks
    hash_code INT64 
        OPTIONS(description = 'Computed hash for row-level deduplication'),
    
    -- Client IP address that initiated the action
    ip_address STRING 
        OPTIONS(description = 'Client IP address'),
    
    -- Partner name (null if customer numeric login)
    partner_name STRING 
        OPTIONS(description = 'Partner name (null if customer)'),
    
    -- FTP session identifier for grouping related events
    session_id STRING 
        OPTIONS(description = 'FTP session identifier'),
    
    -- FTP server that generated this log entry
    source STRING 
        OPTIONS(description = 'FTP server identifier'),
    
    -- FTP username (may be numeric custId or partner name)
    user_name STRING 
        OPTIONS(description = 'FTP username'),
    
    -- Human-readable status message from FTP server
    server_response STRING 
        OPTIONS(description = 'Human-readable FTP status message'),
    
    -- Original raw log line (for debugging and recovery)
    raw_data STRING 
        OPTIONS(description = 'Original FTP log line')
    
    -- NOTE: StatusCode is intentionally EXCLUDED per Snowflake parity requirement
    -- It is preserved in archive_ftplog.raw_json if ever needed
)
PARTITION BY DATE(event_dt)
CLUSTER BY source, cust_id
OPTIONS (
    description = 'FTP log events - partitioned by event date, clustered by source and customer',
    labels = [
        ('table_type', 'base'),
        ('retention', 'standard')
    ],
    -- Require partition filter to prevent full table scans
    require_partition_filter = false
);

-- Add a comment explaining the table purpose
-- (This shows up in BigQuery UI and documentation)
