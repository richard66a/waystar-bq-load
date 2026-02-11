#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# create_scheduled_query.sh
# Builds the JSON params for a scheduled query using the SQL in sql/06_scheduled_query_etl.sql
# and either prints the exact `bq mk` command to run or executes it (if --apply is passed).

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/../sql/06_scheduled_query_etl_scheduled.sql"
CALL_SQL_FILE="${SCRIPT_DIR}/../sql/06_scheduled_query_call.sql"

# Load optional config defaults
if [ -f "${SCRIPT_DIR}/../config/settings.sh" ]; then
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}/../config/settings.sh"
fi

PROJECT="${PROJECT_ID:-your-gcp-project-id}"
DATASET="${DATASET_ID:-logviewer}"
DISPLAY_NAME="FTPlog ETL (every 5m)"
SCHEDULE="${SCHEDULED_QUERY_SCHEDULE:-every 5 minutes}"

if command -v require_cmd >/dev/null 2>&1; then
  require_cmd jq
  require_cmd bq
else
  command -v jq >/dev/null 2>&1 || { echo "Missing required command: jq" >&2; exit 2; }
  command -v bq >/dev/null 2>&1 || { echo "Missing required command: bq" >&2; exit 2; }
fi

usage(){
  cat <<EOF
Usage: $0 [--project PROJECT] [--dataset DATASET] [--display-name NAME] [--schedule SCHEDULE] [--call-proc] [--apply]

Options:
  --project       GCP project id (default: ${PROJECT})
  --dataset       BigQuery dataset for the scheduled query (default: ${DATASET})
  --display-name  Friendly name for the scheduled query (default: "${DISPLAY_NAME}")
  --schedule      Schedule string (default: "${SCHEDULE}")
  --call-proc     Use stored procedure CALL body instead of DML script
  --apply         Execute the generated `bq mk` command. Without --apply the command is printed only.

Example:
  $0 --apply

EOF
  exit 1
}

APPLY=false
USE_CALL=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2;;
    --dataset) DATASET="$2"; shift 2;;
    --display-name) DISPLAY_NAME="$2"; shift 2;;
    --schedule) SCHEDULE="$2"; shift 2;;
    --call-proc) USE_CALL=true; shift 1;;
    --apply) APPLY=true; shift 1;;
    -h|--help) usage;;
    *) echo "Unknown arg: $1"; usage;;
  esac
done

if [ "$USE_CALL" = true ]; then
  SQL_FILE="$CALL_SQL_FILE"
fi

if [[ ! -f "$SQL_FILE" ]]; then
  echo "SQL file not found: $SQL_FILE" >&2
  exit 2
fi

# Read SQL with real newlines to preserve script structure
SQL_CONTENT=$(cat "$SQL_FILE")

# Build params JSON using jq to ensure proper escaping (no write_disposition for DML script)
PARAMS=$(jq -nc --arg q "$SQL_CONTENT" '{query: $q}')

CMD=(bq mk --transfer_config --project_id="$PROJECT" --data_source=scheduled_query \
  --display_name="$DISPLAY_NAME" \
  --params="$PARAMS" \
  --schedule="$SCHEDULE" \
  --target_dataset="$DATASET")

echo "Prepared bq mk command:" 
printf '%s ' "${CMD[@]}"
echo

if [ "$APPLY" = true ]; then
  echo "\nExecuting scheduled query creation (you may be prompted to authorize)..."
  "${CMD[@]}"
  echo "Done. If OAuth consent was required you may have seen an interactive URL in the terminal to authorize." 
else
  echo "\nTo create the scheduled query, run the printed command above. To run it now, re-run with --apply."
fi
