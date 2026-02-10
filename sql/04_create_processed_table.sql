-- =============================================================================
-- FTP Log Pipeline - Step 4: Create Processed Files Tracking Table
-- =============================================================================
-- This table tracks which GCS files have been processed to prevent duplicates.
-- 
-- Design decisions:
--   - Acts as the "ledger" for idempotency
--   - Stores processing metadata for debugging
--   - Status field enables partial failure tracking
--   - gcs_uri is the unique key (checked before processing)
--
-- Usage:
--   bq query --use_legacy_sql=false < 04_create_processed_table.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS `sbox-ravelar-001-20250926.logviewer.processed_files`
(
    -- Full GCS URI (unique identifier for each file)
    gcs_uri STRING NOT NULL 
        OPTIONS(description = 'Full GCS URI of processed file'),
    
    -- LoadId extracted from filename
    originating_filename STRING NOT NULL 
        OPTIONS(description = 'LoadId extracted from filename'),
    
    -- When the file was processed
    processed_timestamp TIMESTAMP NOT NULL 
        OPTIONS(description = 'When file was processed'),
    
    -- Number of rows successfully loaded from this file
    rows_loaded INT64 
        OPTIONS(description = 'Number of rows loaded from this file'),

    -- Number of non-empty rows discovered for this file
    rows_expected INT64 
        OPTIONS(description = 'Number of non-empty rows discovered for this file'),

    -- Rows not loaded (rows_expected - rows_loaded)
    parse_errors INT64 
        OPTIONS(description = 'Number of rows not loaded (rows_expected - rows_loaded)'),
    
    -- Processing status for error tracking
    -- SUCCESS: All rows processed
    -- FAILED: Processing failed entirely
    -- PARTIAL: Some rows failed (malformed JSON)
    status STRING 
        OPTIONS(description = 'Processing status: SUCCESS, FAILED, PARTIAL'),
    
    -- Error message if status is FAILED or PARTIAL
    error_message STRING 
        OPTIONS(description = 'Error details if processing failed'),
    
    -- Processing duration in seconds (for performance monitoring)
    processing_duration_seconds FLOAT64 
        OPTIONS(description = 'Time taken to process this file')
)
OPTIONS (
    description = 'Tracks processed GCS files to prevent duplicate loading',
    labels = [
        ('table_type', 'tracking'),
        ('purpose', 'idempotency')
    ]
);

-- Ensure schema is updated if the table already exists
ALTER TABLE `sbox-ravelar-001-20250926.logviewer.processed_files`
ADD COLUMN IF NOT EXISTS rows_expected INT64 OPTIONS(description = 'Number of non-empty rows discovered for this file');

ALTER TABLE `sbox-ravelar-001-20250926.logviewer.processed_files`
ADD COLUMN IF NOT EXISTS parse_errors INT64 OPTIONS(description = 'Number of rows not loaded (rows_expected - rows_loaded)');

-- Create a unique constraint simulation using a view
-- (BigQuery doesn't support true unique constraints, but we check in ETL)
