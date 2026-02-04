#!/bin/bash
# =============================================================================
# FTP Log Pipeline - Cleanup/Teardown Script
# =============================================================================
# Removes all GCP resources created by the pipeline.
# USE WITH CAUTION - This will delete all data!
#
# Usage:
#   chmod +x teardown.sh
#   ./teardown.sh [--confirm]
# =============================================================================

set -e

# Load configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../config/settings.sh"

echo "=============================================================="
echo "FTP Log Pipeline - Teardown Script"
echo "=============================================================="
echo "Project: ${PROJECT_ID}"
echo "=============================================================="
echo ""
echo "WARNING: This will DELETE all pipeline resources including:"
echo "  - BigQuery dataset: ${DATASET_ID} (and all tables)"
echo "  - GCS bucket: ${GCS_BUCKET} (and all files)"
echo "  - Service account: ${SERVICE_ACCOUNT_NAME}"
echo "  - Scheduled queries"
echo ""

# Check for confirmation flag
if [ "$1" != "--confirm" ]; then
    echo "To proceed, run: $0 --confirm"
    echo ""
    exit 1
fi

read -p "Are you absolutely sure? Type 'DELETE' to confirm: " CONFIRM

if [ "${CONFIRM}" != "DELETE" ]; then
    echo "Aborted."
    exit 1
fi

set_project

echo ""
log_info "Starting teardown..."

# -----------------------------------------------------------------------------
# Step 1: Delete Scheduled Queries
# -----------------------------------------------------------------------------
log_info "Removing scheduled queries..."

# List and delete transfer configs
TRANSFER_CONFIGS=$(bq ls --transfer_config --transfer_location="${BQ_LOCATION}" \
    --project_id="${PROJECT_ID}" \
    --format=json 2>/dev/null || echo "[]")

echo "${TRANSFER_CONFIGS}" | python3 -c "
import sys, json
configs = json.load(sys.stdin) or []
for c in configs:
    if c.get('displayName', '').startswith('process_ftplog'):
        print(c['name'])
" 2>/dev/null | while read -r CONFIG_NAME; do
    if [ -n "${CONFIG_NAME}" ]; then
        log_info "Deleting transfer config: ${CONFIG_NAME}"
        bq rm -f --transfer_config "${CONFIG_NAME}" 2>/dev/null || true
    fi
done

log_success "Scheduled queries removed"

# -----------------------------------------------------------------------------
# Step 2: Delete BigQuery Dataset
# -----------------------------------------------------------------------------
log_info "Removing BigQuery dataset..."

if bq show --project_id="${PROJECT_ID}" "${DATASET_ID}" &>/dev/null; then
    bq rm -r -f --project_id="${PROJECT_ID}" "${DATASET_ID}"
    log_success "Deleted dataset: ${DATASET_ID}"
else
    log_info "Dataset not found: ${DATASET_ID}"
fi

# -----------------------------------------------------------------------------
# Step 3: Delete GCS Bucket
# -----------------------------------------------------------------------------
log_info "Removing GCS bucket..."

if gsutil ls -b "gs://${GCS_BUCKET}" &>/dev/null; then
    gsutil -m rm -r "gs://${GCS_BUCKET}"
    log_success "Deleted bucket: ${GCS_BUCKET}"
else
    log_info "Bucket not found: ${GCS_BUCKET}"
fi

# -----------------------------------------------------------------------------
# Step 4: Delete Service Account
# -----------------------------------------------------------------------------
log_info "Removing service account..."

if gcloud iam service-accounts describe "${SERVICE_ACCOUNT_EMAIL}" &>/dev/null; then
    gcloud iam service-accounts delete "${SERVICE_ACCOUNT_EMAIL}" --quiet
    log_success "Deleted service account: ${SERVICE_ACCOUNT_NAME}"
else
    log_info "Service account not found: ${SERVICE_ACCOUNT_NAME}"
fi

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------
echo ""
echo "=============================================================="
log_success "Teardown complete!"
echo "=============================================================="
echo ""
echo "All pipeline resources have been deleted."
echo "To redeploy, run: ./scripts/deploy.sh"
echo ""
