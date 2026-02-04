#!/bin/bash
# =============================================================================
# FTP Log Pipeline - Deploy Monitoring Alerts
# =============================================================================
# Deploys an HTTP Cloud Function that checks pipeline_monitoring and emits
# PIPELINE_ALERT logs, creates a Cloud Scheduler job to invoke it, and sets up
# a log-based metric + alert policy in Cloud Monitoring.
# =============================================================================

set -e

# Load configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../config/settings.sh"

set_project

echo "=============================================================="
echo "FTP Log Pipeline - Monitoring Alerts Deployment"
echo "=============================================================="
echo "Project: ${PROJECT_ID}"
echo "Function: ${MONITORING_ALERT_FUNCTION_NAME}"
echo "Scheduler job: ${MONITORING_ALERT_SCHEDULER_JOB_NAME}"
echo "Region: ${REGION}"
echo "=============================================================="

# Enable required APIs (idempotent)
log_info "Enabling required APIs..."
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  run.googleapis.com \
  eventarc.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  --project "${PROJECT_ID}" >/dev/null

log_info "Deploying monitoring alert Cloud Function..."
cd "${SCRIPT_DIR}/../cloud_function"

gcloud functions deploy "${MONITORING_ALERT_FUNCTION_NAME}" \
  --gen2 \
  --runtime="${CLOUD_FUNCTION_RUNTIME}" \
  --region="${REGION}" \
  --source=. \
  --entry-point=run_monitoring_alert \
  --trigger-http \
  --no-allow-unauthenticated \
  --service-account="${SERVICE_ACCOUNT_EMAIL}" \
  --memory="${CLOUD_FUNCTION_MEMORY}" \
  --timeout="${CLOUD_FUNCTION_TIMEOUT}" \
  --set-env-vars="PROJECT_ID=${PROJECT_ID}"

FUNCTION_URL=$(gcloud functions describe "${MONITORING_ALERT_FUNCTION_NAME}" \
  --gen2 \
  --region="${REGION}" \
  --format="value(serviceConfig.uri)")

if [ -z "${FUNCTION_URL}" ]; then
  log_error "Failed to resolve monitoring function URL."
  exit 1
fi

log_info "Granting invoker role to ${SERVICE_ACCOUNT_EMAIL}..."
gcloud functions add-invoker-policy-binding "${MONITORING_ALERT_FUNCTION_NAME}" \
  --gen2 \
  --region="${REGION}" \
  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}"

# Create or update scheduler job
if gcloud scheduler jobs describe "${MONITORING_ALERT_SCHEDULER_JOB_NAME}" \
  --location="${REGION}" &>/dev/null; then
  log_info "Updating Cloud Scheduler job..."
  gcloud scheduler jobs update http "${MONITORING_ALERT_SCHEDULER_JOB_NAME}" \
    --location="${REGION}" \
    --schedule="*/5 * * * *" \
    --time-zone="${SCHEDULED_QUERY_TIMEZONE}" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oidc-token-audience="${FUNCTION_URL}"
else
  log_info "Creating Cloud Scheduler job..."
  gcloud scheduler jobs create http "${MONITORING_ALERT_SCHEDULER_JOB_NAME}" \
    --location="${REGION}" \
    --schedule="*/5 * * * *" \
    --time-zone="${SCHEDULED_QUERY_TIMEZONE}" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --oidc-token-audience="${FUNCTION_URL}"
fi

# Create logs-based metric
log_info "Creating logs-based metric ${MONITORING_ALERT_METRIC_NAME}..."
FILTER="resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"${MONITORING_ALERT_FUNCTION_NAME}\" AND textPayload:\"PIPELINE_ALERT\""
if gcloud logging metrics describe "${MONITORING_ALERT_METRIC_NAME}" --project "${PROJECT_ID}" &>/dev/null; then
  log_info "Metric already exists: ${MONITORING_ALERT_METRIC_NAME}"
else
  gcloud logging metrics create "${MONITORING_ALERT_METRIC_NAME}" \
    --project "${PROJECT_ID}" \
    --description "Pipeline alert status from pipeline_monitoring" \
    --log-filter "${FILTER}"
fi

# Create alert policy
log_info "Creating alert policy..."
POLICY_FILE="${SCRIPT_DIR}/../monitoring/alert_policy_pipeline_alert.json"
if [ ! -f "${POLICY_FILE}" ]; then
  log_error "Policy file not found: ${POLICY_FILE}"
  exit 1
fi

# Avoid duplicate policies by checking displayName
EXISTING_POLICY=$(gcloud alpha monitoring policies list \
  --project "${PROJECT_ID}" \
  --format="value(displayName)" | grep -x "FTPlog Pipeline Alerts" || true)

if [ -n "${EXISTING_POLICY}" ]; then
  log_info "Alert policy already exists: FTPlog Pipeline Alerts"
else
  gcloud alpha monitoring policies create \
    --project "${PROJECT_ID}" \
    --policy-from-file "${POLICY_FILE}"
fi

log_success "Monitoring alerts deployment complete."
