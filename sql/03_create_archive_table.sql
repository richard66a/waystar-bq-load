-- =============================================================================
-- FTP Log Pipeline - Step 3: Create Archive Table
-- =============================================================================
-- This table stores raw JSON for compliance, auditing, and recovery.
-- 
-- Design decisions:
--   - Stores complete raw JSON (enables schema evolution recovery)
--   - Partitioned by archived_timestamp (for lifecycle management)
--   - NEVER DELETE data from this table (compliance requirement)
--   - Minimal processing - just store the line as-is
--
-- Usage:
--   bq query --use_legacy_sql=false < 03_create_archive_table.sql
-- =============================================================================

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.__DATASET_ID__.archive_ftplog`
(
    -- Complete raw JSON line from source file (no parsing applied)
    raw_json STRING NOT NULL 
        OPTIONS(description = 'Complete raw JSON line from source file'),
    
    -- When this record was written to the archive
    archived_timestamp TIMESTAMP NOT NULL 
        OPTIONS(description = 'When this record was archived'),
    
    -- Processing timestamp for tracking ETL runs
    process_dt TIMESTAMP NOT NULL 
        OPTIONS(description = 'ETL processing timestamp'),
    
    -- Source filename for recovery operations
    originating_filename STRING NOT NULL 
        OPTIONS(description = 'Source file loadId'),
    
    -- Full GCS path for file-level recovery
    gcs_uri STRING 
        OPTIONS(description = 'Full GCS path for recovery')
)
PARTITION BY DATE(archived_timestamp)
OPTIONS (
    description = 'Permanent archive of raw JSON FTP log events - NEVER DELETE',
    labels = [
        ('table_type', 'archive'),
        ('retention', 'permanent'),
        ('compliance', 'required')
    ]
);
