#!/usr/bin/env bash
# =============================================================================
# FTP Log Pipeline - Example Configuration
# =============================================================================
# Copy this file to config/settings.sh and customize the values for your env.
# =============================================================================

# GCP Project Configuration
# Try to preserve any pre-set environment values; fall back to sensible defaults.
# PROJECT_ID is auto-detected from gcloud if available but not exported unless set.
export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null || true)}"
export REGION="${REGION:-us-central1}"
export BQ_LOCATION="${BQ_LOCATION:-US}"

# BigQuery Configuration
export DATASET_ID="${DATASET_ID:-logviewer}"
export DATASET_DESCRIPTION="${DATASET_DESCRIPTION:-FTP log data - GCP-native pipeline}"

# Table names
export BASE_TABLE="${BASE_TABLE:-base_ftplog}"
export ARCHIVE_TABLE="${ARCHIVE_TABLE:-archive_ftplog}"
export PROCESSED_TABLE="${PROCESSED_TABLE:-processed_files}"
export EXTERNAL_TABLE="${EXTERNAL_TABLE:-external_ftplog_files}"

# GCS Configuration
export GCS_BUCKET="${GCS_BUCKET:-${PROJECT_ID}-ftplog}"
export GCS_LOGS_PREFIX="${GCS_LOGS_PREFIX:-logs}"
export GCS_URI="${GCS_URI:-gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/*.json}"

# Service Account Configuration
export SERVICE_ACCOUNT_NAME="${SERVICE_ACCOUNT_NAME:-sa-logviewer}"
# Build email only if not explicitly provided
export SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_EMAIL:-${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"

# Scheduled Query Configuration
export SCHEDULED_QUERY_NAME="process_ftplog_files"
export SCHEDULED_QUERY_SCHEDULE="every 5 minutes"
export SCHEDULED_QUERY_TIMEZONE="America/Chicago"

# Cloud Function Configuration (Alternative)
export CLOUD_FUNCTION_NAME="process-ftplog"
export CLOUD_FUNCTION_RUNTIME="python311"
export CLOUD_FUNCTION_MEMORY="512MB"
export CLOUD_FUNCTION_TIMEOUT="300s"

# Scheduled ETL (Cloud Scheduler + HTTP Function)
export SCHEDULED_ETL_FUNCTION_NAME="process-ftplog-etl"
export CLOUD_SCHEDULER_JOB_NAME="ftplog-etl-every-5m"

# Monitoring Alert (Cloud Scheduler + HTTP Function)
export MONITORING_ALERT_FUNCTION_NAME="process-ftplog-monitoring"
export MONITORING_ALERT_SCHEDULER_JOB_NAME="ftplog-monitoring-alerts-every-5m"
export MONITORING_ALERT_METRIC_NAME="ftplog_pipeline_alerts"

# Derived Fully Qualified Table Names
export FQ_DATASET="${FQ_DATASET:-${PROJECT_ID}.${DATASET_ID}}"
export FQ_BASE_TABLE="${FQ_BASE_TABLE:-${FQ_DATASET}.${BASE_TABLE}}"
export FQ_ARCHIVE_TABLE="${FQ_ARCHIVE_TABLE:-${FQ_DATASET}.${ARCHIVE_TABLE}}"
export FQ_PROCESSED_TABLE="${FQ_PROCESSED_TABLE:-${FQ_DATASET}.${PROCESSED_TABLE}}"
export FQ_EXTERNAL_TABLE="${FQ_EXTERNAL_TABLE:-${FQ_DATASET}.${EXTERNAL_TABLE}}"

# Lightweight validation summary (printed when the file is sourced)
_print_settings_summary() {
  log_info "Using PROJECT_ID=${PROJECT_ID:-<unset>}, DATASET_ID=${DATASET_ID}, GCS_BUCKET=${GCS_BUCKET}"
}

# Print summary on source, but do not exit — scripts can still override values before calling set_project().
# summary will be printed after helper functions are available

# Helper Functions
log_info() {
  echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
  echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') - $1" >&2
}

log_success() {
  echo "[SUCCESS] $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    log_error "Missing required command: $1"
    return 1
  }
}

check_gcloud_auth() {
  if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    log_error "No active gcloud account. Run: gcloud auth login"
    return 1
  fi
  return 0
}

set_project() {
  gcloud config set project "${PROJECT_ID}" 2>/dev/null
  log_info "Set active project to: ${PROJECT_ID}"
}

# Print settings summary now that helper functions are defined
_print_settings_summary
