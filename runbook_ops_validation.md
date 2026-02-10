# Ops Validation Runbook — NDJSON to BigQuery Pipeline

This runbook validates end-to-end ingestion from GCS NDJSON files into BigQuery base/archive tables and the processed ledger.

## Inputs
- Project: sbox-ravelar-001-20250926
- Dataset: logviewer
- Bucket: sbox-ravelar-001-20250926-ftplog
- Path prefix: logs
- Sample NDJSON file: Waystar-20260203-1212568611-9b9dc78e-4f1a-4d03-881c-0104a5352ccf.json

## One-command E2E test
Use the included script to upload a sample NDJSON file, create or refresh tables, run ETL, and validate results.

```bash
bash waystar-bq-load/scripts/e2e_gcp_test.sh
```

You can override defaults with environment variables:

```bash
PROJECT_ID=sbox-ravelar-001-20250926 \
DATASET=logviewer \
BUCKET=sbox-ravelar-001-20250926-ftplog \
PATH_PREFIX=logs \
SAMPLE_FILE=/path/to/file.json \
bash waystar-bq-load/scripts/e2e_gcp_test.sh
```

## Manual validation steps

### 0) Run the standard validation queries
Use the prebuilt query pack in [waystar-bq-load/sql/validation_queries.sql](waystar-bq-load/sql/validation_queries.sql).

```bash
bq query --use_legacy_sql=false --project_id=sbox-ravelar-001-20250926 < \
  waystar-bq-load/sql/validation_queries.sql
```

### 1) Upload a sample NDJSON file
```bash
gsutil cp /path/to/sample.json gs://sbox-ravelar-001-20250926-ftplog/logs/sample.json
```

### 2) Create/refresh tables and external table
Uses [waystar-bq-load/sql/00_setup_all.sql](waystar-bq-load/sql/00_setup_all.sql) and replaces the `__GCS_URI__` placeholder.

```bash
sed "s|__GCS_URI__|gs://sbox-ravelar-001-20250926-ftplog/logs/*.json|" \
  waystar-bq-load/sql/00_setup_all.sql | \
  bq query --use_legacy_sql=false --project_id=sbox-ravelar-001-20250926
```

### 3) Create ETL stored procedure
Uses [waystar-bq-load/sql/06_scheduled_query_proc.sql](waystar-bq-load/sql/06_scheduled_query_proc.sql).

```bash
bq query --use_legacy_sql=false --project_id=sbox-ravelar-001-20250926 < \
  waystar-bq-load/sql/06_scheduled_query_proc.sql
```

### 4) Verify external table discovery
```sql
SELECT _FILE_NAME, COUNT(*) AS row_count
FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`
WHERE _FILE_NAME = 'gs://sbox-ravelar-001-20250926-ftplog/logs/sample.json'
GROUP BY _FILE_NAME;
```

### 5) Run the ETL
```sql
CALL `sbox-ravelar-001-20250926.logviewer.proc_process_ftplog`();
```

### 6) Confirm ledger and row counts
```sql
SELECT gcs_uri, originating_filename, processed_timestamp, rows_loaded, status
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
WHERE gcs_uri = 'gs://sbox-ravelar-001-20250926-ftplog/logs/sample.json';

SELECT COUNT(*) AS base_rows
FROM `sbox-ravelar-001-20250926.logviewer.base_ftplog`
WHERE gcs_uri = 'gs://sbox-ravelar-001-20250926-ftplog/logs/sample.json';

SELECT COUNT(*) AS archive_rows
FROM `sbox-ravelar-001-20250926.logviewer.archive_ftplog`
WHERE gcs_uri = 'gs://sbox-ravelar-001-20250926-ftplog/logs/sample.json';
```

## Notes
- The ETL uses safe parsing, so malformed fields become NULL rather than failing the job.
- Raw JSON is preserved in `archive_ftplog` for audit and recovery.
- `_FILE_NAME` provides the exact gs:// path used as the idempotency key in `processed_files`.
- `processed_files` now tracks `rows_expected`, `parse_errors`, and `status` to surface files that need investigation.
