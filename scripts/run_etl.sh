#!/usr/bin/env bash
# =============================================================================
# FTP Log Pipeline - Run ETL Manually
# =============================================================================
# Executes the ETL query manually (outside of scheduled execution).
# Useful for testing and initial validation.
#
# Usage:
#   chmod +x run_etl.sh
#   ./run_etl.sh
# =============================================================================

set -euo pipefail
IFS=$'\n\t'

# Load configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/../config/settings.sh"

require_cmd bq

echo "=============================================================="
echo "FTP Log Pipeline - Manual ETL Execution"
echo "=============================================================="
echo "Project: ${PROJECT_ID}"
echo "Time: $(date)"
echo "=============================================================="

set_project

# Check for unprocessed files first
log_info "Checking for unprocessed files..."

UNPROCESSED=$(bq query --use_legacy_sql=false --format=csv --quiet \
    "SELECT COUNT(DISTINCT _FILE_NAME) AS cnt
     FROM \`${FQ_EXTERNAL_TABLE}\`
     WHERE NOT EXISTS (
         SELECT 1 FROM \`${FQ_PROCESSED_TABLE}\` pf
         WHERE pf.gcs_uri = _FILE_NAME
     )" 2>/dev/null | tail -1)

UNPROCESSED="${UNPROCESSED:-0}"

log_info "Unprocessed files found: ${UNPROCESSED}"

if [ "${UNPROCESSED}" = "0" ]; then
    log_info "No new files to process. Exiting."
    exit 0
fi

# Run the ETL
log_info "Running ETL query..."
START_TIME=$(date +%s)

bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" \
    < "${SCRIPT_DIR}/../sql/06_scheduled_query_etl.sql"

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

log_success "ETL completed in ${DURATION} seconds"

# Show results
log_info "Fetching processing summary..."

bq query --use_legacy_sql=false --format=pretty \
    "SELECT 
        gcs_uri,
        rows_loaded,
        status,
        processed_timestamp
     FROM \`${FQ_PROCESSED_TABLE}\`
     ORDER BY processed_timestamp DESC
     LIMIT 5"

echo ""
log_success "ETL execution complete"
