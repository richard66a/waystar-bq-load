#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${PROJECT_ID:-sbox-ravelar-001-20250926}"
DATASET="${DATASET:-logviewer}"
BUCKET="${BUCKET:-sbox-ravelar-001-20250926-ftplog}"
PATH_PREFIX="${PATH_PREFIX:-logs}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="${SCRIPT_DIR}/../sql"

DEFAULT_SAMPLE_FILE="${SCRIPT_DIR}/../tests/sample_data/FTP-SERVER-01-20260128-103000001-test-sample-001.json"
SAMPLE_FILE="${SAMPLE_FILE:-${DEFAULT_SAMPLE_FILE}}"

if [[ ! -f "${SAMPLE_FILE}" ]]; then
  echo "Sample file not found: ${SAMPLE_FILE}" >&2
  echo "Set SAMPLE_FILE to a local NDJSON file path." >&2
  exit 1
fi

FILENAME="$(basename "${SAMPLE_FILE}")"
GCS_URI="gs://${BUCKET}/${PATH_PREFIX}/${FILENAME}"
GCS_WILDCARD="gs://${BUCKET}/${PATH_PREFIX}/*.json"

echo "Uploading sample file to ${GCS_URI}"

gsutil cp "${SAMPLE_FILE}" "${GCS_URI}"

echo "Creating dataset/tables/external table using ${GCS_WILDCARD}"
sed "s|__GCS_URI__|${GCS_WILDCARD}|" "${SQL_DIR}/00_setup_all.sql" | \
  bq query --use_legacy_sql=false --project_id="${PROJECT_ID}"

echo "Creating ETL stored procedure"
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" < "${SQL_DIR}/06_scheduled_query_proc.sql"

echo "External table discovery check"
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" \
  "SELECT _FILE_NAME, COUNT(*) AS row_count FROM \`${PROJECT_ID}.${DATASET}.external_ftplog_files\` WHERE _FILE_NAME = '${GCS_URI}' GROUP BY _FILE_NAME"

echo "Running ETL"
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" \
  "CALL \`${PROJECT_ID}.${DATASET}.proc_process_ftplog\`();"

echo "Validating processed_files ledger"
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" \
  "SELECT gcs_uri, originating_filename, processed_timestamp, rows_loaded, status FROM \`${PROJECT_ID}.${DATASET}.processed_files\` WHERE gcs_uri = '${GCS_URI}'"

echo "Validating base and archive row counts"
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" \
  "SELECT COUNT(*) AS base_rows FROM \`${PROJECT_ID}.${DATASET}.base_ftplog\` WHERE gcs_uri = '${GCS_URI}'"

bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" \
  "SELECT COUNT(*) AS archive_rows FROM \`${PROJECT_ID}.${DATASET}.archive_ftplog\` WHERE gcs_uri = '${GCS_URI}'"

echo "Sample JSON field inspection"
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" \
  "SELECT raw_json, JSON_VALUE(raw_json, '$.EventDt') AS event_dt_text, JSON_VALUE(raw_json, '$.StatusCode') AS status_text, SAFE_CAST(JSON_VALUE(raw_json, '$.Bytes') AS INT64) AS bytes_int FROM \`${PROJECT_ID}.${DATASET}.archive_ftplog\` WHERE originating_filename = REPLACE('${FILENAME}', '.json', '') LIMIT 5"

echo "Done."
