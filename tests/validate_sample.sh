#!/usr/bin/env bash
set -euo pipefail

# validate_sample.sh
# Usage: validate_sample.sh [--bucket BUCKET] [--project PROJECT] [--dataset DATASET]
#
# Runs local validation of NDJSON samples, optionally uploads to GCS, deploys pipeline,
# triggers a manual ETL run, and runs a few BigQuery validation queries.

usage(){
  cat <<EOF
Usage: $0 [--bucket BUCKET] [--project PROJECT] [--dataset DATASET]

If --bucket is provided, the script will upload discovered sample files to gs://BUCKET/logs/.
If --project and --dataset are provided, the script will attempt to run BigQuery validation queries.

Examples:
  $0 --bucket my-test-bucket --project my-proj --dataset ftplog_ds
  $0                # just run local JSON validation
EOF
  exit 1
}

BUCKET=""
PROJECT=""
DATASET=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bucket) BUCKET="$2"; shift 2;;
    --project) PROJECT="$2"; shift 2;;
    --dataset) DATASET="$2"; shift 2;;
    -h|--help) usage;;
    *) echo "Unknown arg: $1"; usage;;
  esac
done

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "Running local NDJSON validator..."
python3 tests/validate_local_samples.py || {
  echo "Local validation failed (see output). Fix samples or update ETL."
  exit 2
}
echo "Local validation passed."

if [[ -n "$BUCKET" ]]; then
  echo "Uploading sample files to gs://$BUCKET/logs/"
  mkdir -p /tmp/ftplog_uploads
  # find same candidates as the python script
  for f in Waystar-*.json *.ndjson *.json; do
    if [[ -f "$f" ]]; then
      echo "  uploading $f"
      gsutil cp "$f" "gs://$BUCKET/logs/"
    fi
  done
  echo "Upload complete."

  echo "Deploying pipeline (creates dataset/tables and external table)..."
  ./scripts/deploy.sh

  echo "Running manual ETL runner..."
  ./scripts/run_etl.sh

  if [[ -n "$PROJECT" && -n "$DATASET" ]]; then
    echo "Running BigQuery validation queries (project=$PROJECT dataset=$DATASET)"

    echo "Processed files:" 
    bq query --nouse_legacy_sql --format=prettyjson "SELECT gcs_uri, originating_filename, processed_at FROM \\`$PROJECT.$DATASET.processed_files\\` ORDER BY processed_at DESC LIMIT 10"

    echo "Parsed rows count:"
    bq query --nouse_legacy_sql --format=prettyjson "SELECT COUNT(*) AS parsed_rows FROM \\`$PROJECT.$DATASET.base_ftplog\\`"

    echo "Sample rows from base_ftplog:"
    bq query --nouse_legacy_sql --format=prettyjson "SELECT cust_id, event_dt, action, filename, source FROM \\`$PROJECT.$DATASET.base_ftplog\\` ORDER BY event_dt DESC LIMIT 10"

    echo "Sample archive entries:"
    bq query --nouse_legacy_sql --format=prettyjson "SELECT originating_filename, raw_json FROM \\`$PROJECT.$DATASET.archive_ftplog\\` LIMIT 5"
  else
    echo "Skipping BigQuery validation queries because --project/--dataset not provided."
  fi
else
  echo "No --bucket provided; upload + pipeline deployment skipped. To fully validate end-to-end, run with --bucket, --project and --dataset set."
fi

echo "Done."
