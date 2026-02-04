#!/bin/bash
# =============================================================================
# FTP Log Pipeline - Configuration Settings
# =============================================================================
# This file contains all configurable parameters for the pipeline.
# Update these values for your environment before deployment.
# =============================================================================

# -----------------------------------------------------------------------------
# GCP Project Configuration
# -----------------------------------------------------------------------------
export PROJECT_ID="sbox-ravelar-001-20250926"
export REGION="us-central1"
export BQ_LOCATION="US"

# -----------------------------------------------------------------------------
# BigQuery Configuration
# -----------------------------------------------------------------------------
export DATASET_ID="logviewer"
export DATASET_DESCRIPTION="FTP log data - GCP-native pipeline"

# Table names
export BASE_TABLE="base_ftplog"
export ARCHIVE_TABLE="archive_ftplog"
export PROCESSED_TABLE="processed_files"
export EXTERNAL_TABLE="external_ftplog_files"

# -----------------------------------------------------------------------------
# GCS Configuration
# -----------------------------------------------------------------------------
export GCS_BUCKET="${PROJECT_ID}-ftplog"
export GCS_LOGS_PREFIX="logs"
export GCS_URI="gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/*.json"

# -----------------------------------------------------------------------------
# Service Account Configuration
# -----------------------------------------------------------------------------
export SERVICE_ACCOUNT_NAME="sa-logviewer"
export SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# -----------------------------------------------------------------------------
# Scheduled Query Configuration
# -----------------------------------------------------------------------------
export SCHEDULED_QUERY_NAME="process_ftplog_files"
export SCHEDULED_QUERY_SCHEDULE="every 5 minutes"
export SCHEDULED_QUERY_TIMEZONE="America/Chicago"

# -----------------------------------------------------------------------------
# Cloud Function Configuration (Alternative)
# -----------------------------------------------------------------------------
export CLOUD_FUNCTION_NAME="process-ftplog"
export CLOUD_FUNCTION_RUNTIME="python311"
export CLOUD_FUNCTION_MEMORY="512MB"
export CLOUD_FUNCTION_TIMEOUT="300s"

# -----------------------------------------------------------------------------
# Scheduled ETL (Cloud Scheduler + HTTP Function)
# -----------------------------------------------------------------------------
export SCHEDULED_ETL_FUNCTION_NAME="process-ftplog-etl"
export CLOUD_SCHEDULER_JOB_NAME="ftplog-etl-every-5m"

# -----------------------------------------------------------------------------
# Derived Fully Qualified Table Names
# -----------------------------------------------------------------------------
export FQ_DATASET="${PROJECT_ID}.${DATASET_ID}"
export FQ_BASE_TABLE="${FQ_DATASET}.${BASE_TABLE}"
export FQ_ARCHIVE_TABLE="${FQ_DATASET}.${ARCHIVE_TABLE}"
export FQ_PROCESSED_TABLE="${FQ_DATASET}.${PROCESSED_TABLE}"
export FQ_EXTERNAL_TABLE="${FQ_DATASET}.${EXTERNAL_TABLE}"

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
