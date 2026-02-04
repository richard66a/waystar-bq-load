-- =============================================================================
-- FTP Log Pipeline - Step 5: Create External Table
-- =============================================================================
-- This external table provides a "window" into GCS files without copying data.
-- 
-- Design decisions:
--   - Uses CSV format with unit separator delimiter to read entire line as one field
--   - This handles NDJSON correctly (one JSON object per line)
--   - _FILE_NAME pseudo-column identifies which file each row came from
--   - No data copying = cost efficient and always current
--
-- IMPORTANT: Update the GCS bucket name before running!
--
-- Usage:
--   bq query --use_legacy_sql=false < 05_create_external_table.sql
-- =============================================================================

-- Drop existing external table if it exists (external tables need recreation for URI changes)
DROP EXTERNAL TABLE IF EXISTS `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`;

-- Create external table pointing to GCS
-- The 'data' column will contain the entire JSON line as a string
CREATE EXTERNAL TABLE `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`
(
    -- Each line from the NDJSON file is read as a single string
    data STRING OPTIONS(description = 'Raw JSON line from NDJSON file')
)
OPTIONS (
    format = 'CSV',
    -- Use a rare ASCII Unit Separator so each line is read as a single field
    field_delimiter = '\u001F',
    skip_leading_rows = 0,
    allow_quoted_newlines = true,
    -- Placeholder token: __GCS_URI__  (will be replaced at deploy time by scripts/deploy.sh)
    uris = ['__GCS_URI__'],
    description = 'External table reading NDJSON files from GCS as raw lines'
);

-- Verify external table was created
SELECT
    table_name,
    table_type,
    creation_time
FROM `sbox-ravelar-001-20250926.logviewer.INFORMATION_SCHEMA.TABLES`
WHERE table_name = 'external_ftplog_files';
