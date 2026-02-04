-- =============================================================================
-- FTP Log Pipeline - Step 1: Create BigQuery Dataset
-- =============================================================================
-- This script creates the BigQuery dataset that will contain all pipeline tables.
-- 
-- Usage:
--   bq query --use_legacy_sql=false < 01_create_dataset.sql
-- 
-- Prerequisites:
--   - GCP project exists
--   - User has bigquery.datasets.create permission
-- =============================================================================

-- Create the logviewer dataset
CREATE SCHEMA IF NOT EXISTS `sbox-ravelar-001-20250926.logviewer`
OPTIONS (
    description = 'FTP log data - GCP-native pipeline migrated from Snowflake',
    location = 'US',
    labels = [
        ('environment', 'sandbox'),
        ('team', 'platform-arch'),
        ('pipeline', 'ftplog')
    ]
);

-- Verify creation
SELECT
    catalog_name AS project_id,
    schema_name AS dataset_id,
    location,
    creation_time,
    last_modified_time
FROM `sbox-ravelar-001-20250926.INFORMATION_SCHEMA.SCHEMATA`
WHERE schema_name = 'logviewer';
