#!/bin/bash
# =============================================================================
# FTP Log Pipeline - Deploy Cloud Function (Alternative to Scheduled Query)
# =============================================================================
# Deploys the Cloud Function for near real-time processing.
# Use this INSTEAD of the scheduled query if you need <1 min latency.
#
# Usage:
#   chmod +x deploy_cloud_function.sh
#   ./deploy_cloud_function.sh
# =============================================================================

set -e

# Load configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../config/settings.sh"

echo "=============================================================="
echo "FTP Log Pipeline - Cloud Function Deployment"
echo "=============================================================="
echo "Project: ${PROJECT_ID}"
echo "Function: ${CLOUD_FUNCTION_NAME}"
echo "Bucket: ${GCS_BUCKET}"
echo "=============================================================="

set_project

# Check if bucket exists
if ! gsutil ls -b "gs://${GCS_BUCKET}" &>/dev/null; then
    log_error "GCS bucket does not exist: ${GCS_BUCKET}"
    log_error "Run ./scripts/deploy.sh first to create infrastructure."
    exit 1
fi

# Check if BigQuery tables exist
if ! bq show --project_id="${PROJECT_ID}" "${DATASET_ID}.${BASE_TABLE}" &>/dev/null; then
    log_error "BigQuery tables do not exist."
    log_error "Run ./scripts/deploy.sh first to create infrastructure."
    exit 1
fi

log_info "Deploying Cloud Function..."

cd "${SCRIPT_DIR}/../cloud_function"

gcloud functions deploy "${CLOUD_FUNCTION_NAME}" \
    --gen2 \
    --runtime="${CLOUD_FUNCTION_RUNTIME}" \
    --region="${REGION}" \
    --source=. \
    --entry-point=process_ftplog \
    --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
    --trigger-event-filters="bucket=${GCS_BUCKET}" \
    --service-account="${SERVICE_ACCOUNT_EMAIL}" \
    --memory="${CLOUD_FUNCTION_MEMORY}" \
    --timeout="${CLOUD_FUNCTION_TIMEOUT}" \
    --set-env-vars="PROJECT_ID=${PROJECT_ID}"

log_success "Cloud Function deployed!"

# Verify deployment
log_info "Verifying deployment..."

gcloud functions describe "${CLOUD_FUNCTION_NAME}" \
    --gen2 \
    --region="${REGION}" \
    --format="table(name,state,updateTime)"

echo ""
log_success "Cloud Function is ready!"
echo ""
echo "The function will automatically trigger when files are uploaded to:"
echo "  gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/*.json"
echo ""
echo "To test:"
echo "  gsutil cp tests/sample_data/*.json gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/"
echo ""
echo "To view logs:"
echo "  gcloud functions logs read ${CLOUD_FUNCTION_NAME} --gen2 --region=${REGION}"
echo ""
