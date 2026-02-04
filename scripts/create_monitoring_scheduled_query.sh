#!/usr/bin/env bash
set -euo pipefail

# create_monitoring_scheduled_query.sh
# Builds the JSON params for a monitoring scheduled query using
# sql/07_pipeline_monitoring_insert.sql and either prints the exact
# `bq mk` command to run or executes it (if --apply is passed).

SQL_FILE="$(dirname "$0")/../sql/07_pipeline_monitoring_insert.sql"
PROJECT="sbox-ravelar-001-20250926"
DATASET="logviewer"
DISPLAY_NAME="FTPlog Pipeline Monitoring (every 5m)"
SCHEDULE="every 5 minutes"

usage(){
  cat <<EOF
Usage: $0 [--project PROJECT] [--dataset DATASET] [--display-name NAME] [--schedule SCHEDULE] [--apply]

Options:
  --project       GCP project id (default: ${PROJECT})
  --dataset       BigQuery dataset for the scheduled query (default: ${DATASET})
  --display-name  Friendly name for the scheduled query (default: "${DISPLAY_NAME}")
  --schedule      Schedule string (default: "${SCHEDULE}")
  --apply         Execute the generated `bq mk` command. Without --apply the command is printed only.

Example:
  $0 --apply

EOF
  exit 1
}

APPLY=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2;;
    --dataset) DATASET="$2"; shift 2;;
    --display-name) DISPLAY_NAME="$2"; shift 2;;
    --schedule) SCHEDULE="$2"; shift 2;;
    --apply) APPLY=true; shift 1;;
    -h|--help) usage;;
    *) echo "Unknown arg: $1"; usage;;
  esac
done

if [[ ! -f "$SQL_FILE" ]]; then
  echo "SQL file not found: $SQL_FILE" >&2
  exit 2
fi

SQL_CONTENT=$(cat "$SQL_FILE")
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
