-- =============================================================================
-- FTP Log Pipeline - Scheduled Query Procedure
-- =============================================================================
-- Wraps the multi-statement ETL into a stored procedure for reliable scheduling.
--
-- Usage (one-time deploy):
--   bq query --use_legacy_sql=false < 06_scheduled_query_proc.sql
--
-- Usage (scheduled query body):
--   CALL `__PROJECT_ID__.__DATASET_ID__.proc_process_ftplog`();
-- =============================================================================

CREATE OR REPLACE PROCEDURE `__PROJECT_ID__.__DATASET_ID__.proc_process_ftplog`()
BEGIN
    DECLARE run_started TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  -- ===========================================================================
  -- STEP 1: Identify new files not yet processed
  -- ===========================================================================
  CREATE TEMP TABLE _new_files AS
  SELECT DISTINCT
      _FILE_NAME AS file_path,
      REGEXP_EXTRACT(_FILE_NAME, r'/([^/]+)\.json$') AS originating_filename
  FROM `__PROJECT_ID__.__DATASET_ID__.external_ftplog_files`
  WHERE _FILE_NAME IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM `__PROJECT_ID__.__DATASET_ID__.processed_files` pf
        WHERE pf.gcs_uri = _FILE_NAME
    );

  -- Log how many new files found (visible in job details)
  SELECT
      COUNT(*) AS new_files_to_process,
      CURRENT_TIMESTAMP() AS run_timestamp
  FROM _new_files;

  -- Capture expected row counts per file (non-empty lines)
  CREATE TEMP TABLE _file_stats AS
  SELECT
      nf.file_path,
      nf.originating_filename,
      COUNTIF(ext.data IS NOT NULL AND TRIM(ext.data) != '') AS rows_expected
  FROM `__PROJECT_ID__.__DATASET_ID__.external_ftplog_files` ext
  INNER JOIN _new_files nf
      ON ext._FILE_NAME = nf.file_path
  GROUP BY nf.file_path, nf.originating_filename;

  -- ===========================================================================
  -- STEP 2: Parse and load structured data into base table
  -- ===========================================================================
  INSERT INTO `__PROJECT_ID__.__DATASET_ID__.base_ftplog`
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
      hash_fingerprint,
      ip_address,
      partner_name,
      session_id,
      source,
      user_name,
      server_response,
      raw_data
  )
    -- Compute fingerprint per row and insert only if fingerprint not already present
    WITH candidate AS (
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
            TO_HEX(SHA256(CONCAT(
                COALESCE(JSON_VALUE(ext.data, '$.EventDt'), ''), '|',
                COALESCE(JSON_VALUE(ext.data, '$.Source'), ''), '|',
                COALESCE(JSON_VALUE(ext.data, '$.Filename'), ''), '|',
                COALESCE(JSON_VALUE(ext.data, '$.Bytes'), ''), '|',
                COALESCE(JSON_VALUE(ext.data, '$.UserName'), '')
            ))) AS hash_fingerprint,
            JSON_VALUE(ext.data, '$.IpAddress') AS ip_address,
            JSON_VALUE(ext.data, '$.PartnerName') AS partner_name,
            JSON_VALUE(ext.data, '$.SessionId') AS session_id,
            JSON_VALUE(ext.data, '$.Source') AS source,
            JSON_VALUE(ext.data, '$.UserName') AS user_name,
            JSON_VALUE(ext.data, '$.ServerResponse') AS server_response,
            JSON_VALUE(ext.data, '$.RawData') AS raw_data
        FROM `__PROJECT_ID__.__DATASET_ID__.external_ftplog_files` ext
        INNER JOIN _new_files nf
            ON ext._FILE_NAME = nf.file_path
        WHERE
            ext.data IS NOT NULL
            AND TRIM(ext.data) != ''
            AND STARTS_WITH(TRIM(ext.data), '{')
    )
    SELECT
        c.load_time_dt,
        c.source_file_dt,
        c.originating_filename,
        c.gcs_uri,
        c.action,
        c.bytes,
        c.cust_id,
        c.event_dt,
        c.filename,
        c.hash_code,
        c.hash_fingerprint,
        c.ip_address,
        c.partner_name,
        c.session_id,
        c.source,
        c.user_name,
        c.server_response,
        c.raw_data
    FROM candidate c
    LEFT JOIN `__PROJECT_ID__.__DATASET_ID__.base_ftplog` b
        ON b.hash_fingerprint = c.hash_fingerprint
    WHERE b.hash_fingerprint IS NULL;

  -- ===========================================================================
  -- STEP 3: Archive raw JSON for compliance and recovery
  -- ===========================================================================
  INSERT INTO `__PROJECT_ID__.__DATASET_ID__.archive_ftplog`
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
  FROM `__PROJECT_ID__.__DATASET_ID__.external_ftplog_files` ext
  INNER JOIN _new_files nf
      ON ext._FILE_NAME = nf.file_path
  WHERE
      ext.data IS NOT NULL
      AND TRIM(ext.data) != '';

    -- ==========================================================================
    -- STEP 4: Mark files as processed (idempotent)
    -- ==========================================================================
    CREATE TEMP TABLE _file_loads AS
    SELECT
        fs.file_path,
        fs.originating_filename,
        fs.rows_expected,
        (
            SELECT COUNT(*)
            FROM `__PROJECT_ID__.__DATASET_ID__.base_ftplog` b
            WHERE b.gcs_uri = fs.file_path
        ) AS rows_loaded
    FROM _file_stats fs;

    MERGE `__PROJECT_ID__.__DATASET_ID__.processed_files` AS target
    USING (
            SELECT
                    fl.file_path AS gcs_uri,
                    fl.originating_filename,
                    CURRENT_TIMESTAMP() AS processed_timestamp,
                    fl.rows_loaded,
                    fl.rows_expected,
                    GREATEST(fl.rows_expected - fl.rows_loaded, 0) AS parse_errors,
                    CASE
                        WHEN fl.rows_expected = 0 THEN 'FAILED'
                        WHEN fl.rows_loaded = 0 THEN 'FAILED'
                        WHEN GREATEST(fl.rows_expected - fl.rows_loaded, 0) > 0 THEN 'PARTIAL'
                        ELSE 'SUCCESS'
                    END AS status,
                    CASE
                        WHEN fl.rows_expected = 0 THEN 'No non-empty rows found in file'
                        WHEN fl.rows_loaded = 0 THEN 'No rows loaded from file'
                        WHEN GREATEST(fl.rows_expected - fl.rows_loaded, 0) > 0 THEN CONCAT('Parsed ', CAST(fl.rows_loaded AS STRING), ' of ', CAST(fl.rows_expected AS STRING), ' rows')
                        ELSE NULL
                    END AS error_message,
                    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), run_started, SECOND) AS processing_duration_seconds
            FROM _file_loads fl
    ) AS source
    ON target.gcs_uri = source.gcs_uri
    WHEN NOT MATCHED THEN
        INSERT (gcs_uri, originating_filename, processed_timestamp, rows_loaded, rows_expected, parse_errors, status, error_message, processing_duration_seconds)
        VALUES (source.gcs_uri, source.originating_filename, source.processed_timestamp, source.rows_loaded, source.rows_expected, source.parse_errors, source.status, source.error_message, source.processing_duration_seconds);

  -- ===========================================================================
  -- STEP 5: Summary output
  -- ===========================================================================
  SELECT
      'ETL Complete' AS status,
      CURRENT_TIMESTAMP() AS completed_at,
      (SELECT COUNT(*) FROM _new_files) AS files_processed,
      (
          SELECT COUNT(*)
          FROM `__PROJECT_ID__.__DATASET_ID__.base_ftplog`
          WHERE load_time_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
      ) AS rows_loaded_last_hour;
END;
