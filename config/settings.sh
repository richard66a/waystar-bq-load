#!/usr/bin/env bash
# =============================================================================
# FTP Log Pipeline - Configuration Settings
# =============================================================================
# This file contains all configurable parameters for the pipeline.
# Update these values for your environment before deployment.
# =============================================================================

# Optional: local overrides (not committed)
if [ -f "$(dirname "${BASH_SOURCE[0]}")/settings.local.sh" ]; then
    # shellcheck disable=SC1091
    source "$(dirname "${BASH_SOURCE[0]}")/settings.local.sh"
fi

# -----------------------------------------------------------------------------
# GCP Project Configuration
# -----------------------------------------------------------------------------
: "${PROJECT_ID:=your-gcp-project-id}"
: "${REGION:=us-central1}"
: "${BQ_LOCATION:=US}"

# -----------------------------------------------------------------------------
# BigQuery Configuration
# -----------------------------------------------------------------------------
: "${DATASET_ID:=logviewer}"
: "${DATASET_DESCRIPTION:=FTP log data - GCP-native pipeline}"

# Table names
: "${BASE_TABLE:=base_ftplog}"
: "${ARCHIVE_TABLE:=archive_ftplog}"
: "${PROCESSED_TABLE:=processed_files}"
: "${EXTERNAL_TABLE:=external_ftplog_files}"

# -----------------------------------------------------------------------------
# GCS Configuration
# -----------------------------------------------------------------------------
: "${GCS_BUCKET:=${PROJECT_ID}-ftplog}"
: "${GCS_LOGS_PREFIX:=logs}"
: "${GCS_URI:=gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/*.json}"

# -----------------------------------------------------------------------------
# Service Account Configuration
# -----------------------------------------------------------------------------
: "${SERVICE_ACCOUNT_NAME:=sa-logviewer}"
: "${SERVICE_ACCOUNT_EMAIL:=${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com}"

# -----------------------------------------------------------------------------
# Scheduled Query Configuration
# -----------------------------------------------------------------------------
: "${SCHEDULED_QUERY_NAME:=process_ftplog_files}"
: "${SCHEDULED_QUERY_SCHEDULE:=every 5 minutes}"
: "${SCHEDULED_QUERY_TIMEZONE:=America/Chicago}"

# -----------------------------------------------------------------------------
# Cloud Function Configuration (Alternative)
# -----------------------------------------------------------------------------
: "${CLOUD_FUNCTION_NAME:=process-ftplog}"
: "${CLOUD_FUNCTION_RUNTIME:=python311}"
: "${CLOUD_FUNCTION_MEMORY:=512MB}"
: "${CLOUD_FUNCTION_TIMEOUT:=300s}"

# -----------------------------------------------------------------------------
# Scheduled ETL (Cloud Scheduler + HTTP Function)
# -----------------------------------------------------------------------------
: "${SCHEDULED_ETL_FUNCTION_NAME:=process-ftplog-etl}"
: "${CLOUD_SCHEDULER_JOB_NAME:=ftplog-etl-every-5m}"

# -----------------------------------------------------------------------------
# Monitoring Alert (Cloud Scheduler + HTTP Function)
# -----------------------------------------------------------------------------
: "${MONITORING_ALERT_FUNCTION_NAME:=process-ftplog-monitoring}"
: "${MONITORING_ALERT_SCHEDULER_JOB_NAME:=ftplog-monitoring-alerts-every-5m}"
: "${MONITORING_ALERT_METRIC_NAME:=ftplog_pipeline_alerts}"

# -----------------------------------------------------------------------------
# Derived Fully Qualified Table Names
# -----------------------------------------------------------------------------
: "${FQ_DATASET:=${PROJECT_ID}.${DATASET_ID}}"
: "${FQ_BASE_TABLE:=${FQ_DATASET}.${BASE_TABLE}}"
: "${FQ_ARCHIVE_TABLE:=${FQ_DATASET}.${ARCHIVE_TABLE}}"
: "${FQ_PROCESSED_TABLE:=${FQ_DATASET}.${PROCESSED_TABLE}}"
: "${FQ_EXTERNAL_TABLE:=${FQ_DATASET}.${EXTERNAL_TABLE}}"

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------
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
