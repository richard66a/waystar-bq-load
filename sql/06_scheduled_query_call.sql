-- =============================================================================
-- FTP Log Pipeline - Scheduled Query Body (CALL Stored Procedure)
-- =============================================================================
-- Intended for BigQuery Scheduled Query execution if CALL is supported.
-- =============================================================================

CALL `sbox-ravelar-001-20250926.logviewer.proc_process_ftplog`();
