#!/usr/bin/env bash
# =============================================================================
# FTP Log Pipeline - Deploy Scheduled ETL (Cloud Scheduler + HTTP Function)
# =============================================================================
# Deploys an HTTP-triggered Cloud Function to run the ETL SQL and a Cloud
# Scheduler job to invoke it every 5 minutes.
#
# Usage:
#   chmod +x scripts/deploy_scheduled_etl.sh
#   ./scripts/deploy_scheduled_etl.sh
# =============================================================================

set -euo pipefail
IFS=$'\n\t'

# Load configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../config/settings.sh"

require_cmd gcloud
require_cmd bq

echo "=============================================================="
echo "FTP Log Pipeline - Scheduled ETL Deployment"
echo "=============================================================="
echo "Project: ${PROJECT_ID}"
echo "Function: ${SCHEDULED_ETL_FUNCTION_NAME}"
echo "Scheduler job: ${CLOUD_SCHEDULER_JOB_NAME}"
echo "Region: ${REGION}"
echo "=============================================================="

set_project

# Check if BigQuery tables exist
if ! bq show --project_id="${PROJECT_ID}" "${DATASET_ID}.${BASE_TABLE}" &>/dev/null; then
    log_error "BigQuery tables do not exist. Run ./scripts/deploy.sh first."
    exit 1
fi

log_info "Deploying HTTP Cloud Function for scheduled ETL..."

cd "${SCRIPT_DIR}/../cloud_function"

gcloud functions deploy "${SCHEDULED_ETL_FUNCTION_NAME}" \
    --gen2 \
    --runtime="${CLOUD_FUNCTION_RUNTIME}" \
    --region="${REGION}" \
    --source=. \
    --entry-point=run_scheduled_etl \
    --trigger-http \
    --no-allow-unauthenticated \
    --service-account="${SERVICE_ACCOUNT_EMAIL}" \
    --memory="${CLOUD_FUNCTION_MEMORY}" \
    --timeout="${CLOUD_FUNCTION_TIMEOUT}" \
    --set-env-vars="PROJECT_ID=${PROJECT_ID},DATASET_ID=${DATASET_ID},GCS_LOGS_PREFIX=${GCS_LOGS_PREFIX}"

log_success "Scheduled ETL Cloud Function deployed."

# Get function URL
FUNCTION_URL=$(gcloud functions describe "${SCHEDULED_ETL_FUNCTION_NAME}" \
    --gen2 \
    --region="${REGION}" \
    --format="value(serviceConfig.uri)")

if [ -z "${FUNCTION_URL}" ]; then
    log_error "Failed to resolve function URL."
    exit 1
fi

# Allow scheduler to invoke function
log_info "Granting invoker role to ${SERVICE_ACCOUNT_EMAIL}..."
gcloud functions add-invoker-policy-binding "${SCHEDULED_ETL_FUNCTION_NAME}" \
    --gen2 \
    --region="${REGION}" \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}"

# Create or update scheduler job
if gcloud scheduler jobs describe "${CLOUD_SCHEDULER_JOB_NAME}" \
    --location="${REGION}" &>/dev/null; then
    log_info "Updating Cloud Scheduler job..."
    gcloud scheduler jobs update http "${CLOUD_SCHEDULER_JOB_NAME}" \
        --location="${REGION}" \
        --schedule="*/5 * * * *" \
        --time-zone="${SCHEDULED_QUERY_TIMEZONE}" \
        --uri="${FUNCTION_URL}" \
        --http-method=POST \
        --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
        --oidc-token-audience="${FUNCTION_URL}"
else
    log_info "Creating Cloud Scheduler job..."
    gcloud scheduler jobs create http "${CLOUD_SCHEDULER_JOB_NAME}" \
        --location="${REGION}" \
        --schedule="*/5 * * * *" \
        --time-zone="${SCHEDULED_QUERY_TIMEZONE}" \
        --uri="${FUNCTION_URL}" \
        --http-method=POST \
        --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
        --oidc-token-audience="${FUNCTION_URL}"
fi

log_success "Scheduled ETL deployment complete."

echo ""
echo "Next steps:"
echo "  1) Verify the job: gcloud scheduler jobs describe ${CLOUD_SCHEDULER_JOB_NAME} --location ${REGION}"
echo "  2) Trigger an immediate run: gcloud scheduler jobs run ${CLOUD_SCHEDULER_JOB_NAME} --location ${REGION}"
