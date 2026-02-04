#!/bin/bash
# =============================================================================
# FTP Log Pipeline - Full Deployment Script
# =============================================================================
# This script deploys all GCP resources for the FTP Log Pipeline.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - BigQuery API enabled
#   - Cloud Storage API enabled
#   - Appropriate IAM permissions
#
# Usage:
#   chmod +x deploy.sh
#   ./deploy.sh
# =============================================================================

set -e  # Exit on error

# Load configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../config/settings.sh"

echo "=============================================================="
echo "FTP Log Pipeline - Deployment Script"
echo "=============================================================="
echo "Project ID: ${PROJECT_ID}"
echo "Dataset:    ${DATASET_ID}"
echo "GCS Bucket: ${GCS_BUCKET}"
echo "=============================================================="
echo ""

# -----------------------------------------------------------------------------
# Pre-flight checks
# -----------------------------------------------------------------------------
log_info "Running pre-flight checks..."

# Check gcloud authentication
if ! check_gcloud_auth; then
    log_error "Please authenticate with: gcloud auth login"
    exit 1
fi

# Set the project
set_project

# Verify project access
if ! gcloud projects describe "${PROJECT_ID}" &>/dev/null; then
    log_error "Cannot access project: ${PROJECT_ID}"
    log_error "Ensure you have the required permissions."
    exit 1
fi

log_success "Pre-flight checks passed"

# -----------------------------------------------------------------------------
# Step 1: Create GCS Bucket
# -----------------------------------------------------------------------------
echo ""
log_info "Step 1: Creating GCS bucket..."

if gsutil ls -b "gs://${GCS_BUCKET}" &>/dev/null; then
    log_info "Bucket already exists: ${GCS_BUCKET}"
else
    gsutil mb -p "${PROJECT_ID}" -l "${BQ_LOCATION}" "gs://${GCS_BUCKET}"
    log_success "Created bucket: ${GCS_BUCKET}"
fi

# Create logs prefix (folder)
if ! gsutil ls "gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/" &>/dev/null; then
    # Create a placeholder to establish the "folder"
    echo "" | gsutil cp - "gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/.placeholder"
    log_success "Created logs prefix: ${GCS_LOGS_PREFIX}/"
fi

# -----------------------------------------------------------------------------
# Step 2: Create Service Account (if not exists)
# -----------------------------------------------------------------------------
echo ""
log_info "Step 2: Setting up service account..."

if gcloud iam service-accounts describe "${SERVICE_ACCOUNT_EMAIL}" &>/dev/null; then
    log_info "Service account already exists: ${SERVICE_ACCOUNT_NAME}"
else
    gcloud iam service-accounts create "${SERVICE_ACCOUNT_NAME}" \
        --display-name="FTP Log Pipeline Service Account" \
        --description="Service account for FTP Log BigQuery pipeline"
    log_success "Created service account: ${SERVICE_ACCOUNT_NAME}"
fi

# Grant IAM roles
log_info "Granting IAM roles..."

# BigQuery Data Editor - write to tables
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/bigquery.dataEditor" \
    --condition=None \
    --quiet

# BigQuery Job User - run queries
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/bigquery.jobUser" \
    --condition=None \
    --quiet

# Storage Object Viewer - read from GCS
gsutil iam ch "serviceAccount:${SERVICE_ACCOUNT_EMAIL}:objectViewer" "gs://${GCS_BUCKET}"

log_success "IAM roles granted"

# -----------------------------------------------------------------------------
# Step 3: Create BigQuery Dataset and Tables
# -----------------------------------------------------------------------------
echo ""
log_info "Step 3: Creating BigQuery resources..."

# Run the combined setup SQL
cd "${SCRIPT_DIR}/../sql"

log_info "Creating dataset and tables..."
# Substitute the external table URI placeholder with the configured bucket URI
TMP_SQL="/tmp/00_setup_all.$(date +%s).sql"
GCS_URI_REPLACEMENT="gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/*.json"
sed "s|__GCS_URI__|${GCS_URI_REPLACEMENT}|g" 00_setup_all.sql > "${TMP_SQL}"
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" < "${TMP_SQL}"
rm -f "${TMP_SQL}"

log_success "BigQuery resources created"

# -----------------------------------------------------------------------------
# Step 4: Create Scheduled Query
# -----------------------------------------------------------------------------
echo ""
log_info "Step 4: Creating scheduled query..."

# Check if scheduled query already exists
EXISTING_QUERY=$(bq ls --transfer_config --transfer_location="${BQ_LOCATION}" \
    --project_id="${PROJECT_ID}" \
    --format=json 2>/dev/null | \
    python3 -c "import sys,json; configs=json.load(sys.stdin) or []; print(next((c['name'] for c in configs if c.get('displayName')=='${SCHEDULED_QUERY_NAME}'), ''))" 2>/dev/null || echo "")

if [ -n "${EXISTING_QUERY}" ]; then
    log_info "Scheduled query already exists: ${SCHEDULED_QUERY_NAME}"
    log_info "To update, delete and recreate: bq rm -f --transfer_config ${EXISTING_QUERY}"
else
    # Create the scheduled query using the multi-statement SQL with real newlines
    QUERY_FILE="${SCRIPT_DIR}/../sql/06_scheduled_query_etl_scheduled.sql"
    PARAMS=$(python3 - <<PY
import json
from pathlib import Path
sql = Path("${QUERY_FILE}").read_text()
print(json.dumps({"query": sql}))
PY
)

    bq mk --transfer_config \
        --project_id="${PROJECT_ID}" \
        --target_dataset="${DATASET_ID}" \
        --display_name="${SCHEDULED_QUERY_NAME}" \
        --schedule="${SCHEDULED_QUERY_SCHEDULE}" \
        --data_source="scheduled_query" \
        --params="${PARAMS}" \
        2>/dev/null || {
            log_info "Note: Scheduled query creation via CLI may require manual setup."
            log_info "Please create the scheduled query manually in BigQuery Console:"
            log_info "  1. Go to BigQuery > Scheduled Queries"
            log_info "  2. Create new scheduled query"
            log_info "  3. Paste contents of: sql/06_scheduled_query_etl.sql"
            log_info "  4. Schedule: every 5 minutes"
        }
    
    log_success "Scheduled query setup initiated"
fi

# -----------------------------------------------------------------------------
# Step 5: Upload Test Data
# -----------------------------------------------------------------------------
echo ""
log_info "Step 5: Uploading test data..."

SAMPLE_FILE="${SCRIPT_DIR}/../tests/sample_data/FTP-SERVER-01-20260128-103000001-test-sample-001.json"
if [ -f "${SAMPLE_FILE}" ]; then
    gsutil cp "${SAMPLE_FILE}" "gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/"
    log_success "Uploaded test file to GCS"
else
    log_info "No sample file found at: ${SAMPLE_FILE}"
fi

# -----------------------------------------------------------------------------
# Step 6: Verify Deployment
# -----------------------------------------------------------------------------
echo ""
log_info "Step 6: Verifying deployment..."

# Check dataset
if bq show --project_id="${PROJECT_ID}" "${DATASET_ID}" &>/dev/null; then
    log_success "Dataset exists: ${DATASET_ID}"
else
    log_error "Dataset not found: ${DATASET_ID}"
fi

# Check tables
for TABLE in "${BASE_TABLE}" "${ARCHIVE_TABLE}" "${PROCESSED_TABLE}" "${EXTERNAL_TABLE}"; do
    if bq show --project_id="${PROJECT_ID}" "${DATASET_ID}.${TABLE}" &>/dev/null; then
        log_success "Table exists: ${TABLE}"
    else
        log_error "Table not found: ${TABLE}"
    fi
done

# Check GCS bucket
if gsutil ls "gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/" &>/dev/null; then
    FILE_COUNT=$(gsutil ls "gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/*.json" 2>/dev/null | wc -l | tr -d ' ')
    log_success "GCS bucket accessible, ${FILE_COUNT} JSON files found"
else
    log_error "Cannot access GCS bucket"
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo ""
echo "=============================================================="
echo "Deployment Complete!"
echo "=============================================================="
echo ""
echo "Resources created:"
echo "  - GCS Bucket: gs://${GCS_BUCKET}"
echo "  - BigQuery Dataset: ${PROJECT_ID}.${DATASET_ID}"
echo "  - Tables: ${BASE_TABLE}, ${ARCHIVE_TABLE}, ${PROCESSED_TABLE}"
echo "  - External Table: ${EXTERNAL_TABLE}"
echo "  - Service Account: ${SERVICE_ACCOUNT_EMAIL}"
echo ""
echo "Next steps:"
echo "  1. Create scheduled query in BigQuery Console (if not auto-created)"
echo "  2. Generate test data: python tests/generate_test_data.py"
echo "  3. Upload test data: gsutil cp tests/test_files/*.json gs://${GCS_BUCKET}/logs/"
echo "  4. Run ETL manually or wait for scheduled query"
echo "  5. Validate with: sql/validation_queries.sql"
echo ""
echo "For manual ETL execution:"
echo "  bq query --use_legacy_sql=false < sql/06_scheduled_query_etl.sql"
echo ""
