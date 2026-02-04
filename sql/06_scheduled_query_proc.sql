-- =============================================================================
-- FTP Log Pipeline - Scheduled Query Procedure
-- =============================================================================
-- Wraps the multi-statement ETL into a stored procedure for reliable scheduling.
--
-- Usage (one-time deploy):
--   bq query --use_legacy_sql=false < 06_scheduled_query_proc.sql
--
-- Usage (scheduled query body):
--   CALL `sbox-ravelar-001-20250926.logviewer.proc_process_ftplog`();
-- =============================================================================

CREATE OR REPLACE PROCEDURE `sbox-ravelar-001-20250926.logviewer.proc_process_ftplog`()
BEGIN
  -- ===========================================================================
  -- STEP 1: Identify new files not yet processed
  -- ===========================================================================
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

  -- ===========================================================================
  -- STEP 2: Parse and load structured data into base table
  -- ===========================================================================
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
      CURRENT_TIMESTAMP() AS load_time_dt,
      COALESCE(
          SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_VALUE(ext.data, '$.EventDt')),
          CURRENT_TIMESTAMP()
      ) AS source_file_dt,
      nf.originating_filename,
      ext._FILE_NAME AS gcs_uri,
      JSON_VALUE(ext.data, '$.Action') AS action,
      SAFE_CAST(JSON_VALUE(ext.data, '$.Bytes') AS INT64) AS bytes,
      SAFE_CAST(JSON_VALUE(ext.data, '$.CustId') AS INT64) AS cust_id,
      SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', JSON_VALUE(ext.data, '$.EventDt')) AS event_dt,
      JSON_VALUE(ext.data, '$.Filename') AS filename,
      SAFE_CAST(JSON_VALUE(ext.data, '$.HashCode') AS INT64) AS hash_code,
      JSON_VALUE(ext.data, '$.IpAddress') AS ip_address,
      JSON_VALUE(ext.data, '$.PartnerName') AS partner_name,
      JSON_VALUE(ext.data, '$.SessionId') AS session_id,
      JSON_VALUE(ext.data, '$.Source') AS source,
      JSON_VALUE(ext.data, '$.UserName') AS user_name,
      JSON_VALUE(ext.data, '$.ServerResponse') AS server_response,
      JSON_VALUE(ext.data, '$.RawData') AS raw_data
  FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files` ext
  INNER JOIN _new_files nf
      ON ext._FILE_NAME = nf.file_path
  WHERE
      ext.data IS NOT NULL
      AND TRIM(ext.data) != ''
      AND STARTS_WITH(TRIM(ext.data), '{');

  -- ===========================================================================
  -- STEP 3: Archive raw JSON for compliance and recovery
  -- ===========================================================================
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

  -- ===========================================================================
  -- STEP 4: Mark files as processed
  -- ===========================================================================
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
      (
          SELECT COUNT(*)
          FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog` b
          WHERE b.gcs_uri = nf.file_path
            AND b.load_time_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
      ) AS rows_loaded,
      'SUCCESS' AS status
  FROM _new_files nf;

  -- ===========================================================================
  -- STEP 5: Summary output
  -- ===========================================================================
  SELECT
      'ETL Complete' AS status,
      CURRENT_TIMESTAMP() AS completed_at,
      (SELECT COUNT(*) FROM _new_files) AS files_processed,
      (
          SELECT COUNT(*)
          FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
          WHERE load_time_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
      ) AS rows_loaded_last_hour;
END;
