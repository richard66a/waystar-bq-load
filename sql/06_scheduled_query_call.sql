-- =============================================================================
-- FTP Log Pipeline - Scheduled Query Body (CALL Stored Procedure)
-- =============================================================================
-- Intended for BigQuery Scheduled Query execution if CALL is supported.
-- =============================================================================

CALL `__PROJECT_ID__.__DATASET_ID__.proc_process_ftplog`();
